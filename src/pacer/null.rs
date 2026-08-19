use std::collections::HashMap;
use std::time::Instant;

use super::Pacer;
use super::PaddingRequest;
use super::QueueState;
use crate::Reason;
use crate::pacer::PacerReason;
use crate::rtp_::{Bitrate, DataSize, MidRid, TwccClusterId};

/// A null pacer that doesn't pace.
#[derive(Debug)]
pub struct NullPacer {
    last_sends: HashMap<MidRid, Instant>,
    queue_states: Vec<QueueState>,
    needs_timeout_before_next_poll: bool,
    batch: usize,
    current: Option<MidRid>,
    sent_in_batch: usize,
}

impl NullPacer {
    /// Create a pacer that drains up to `batch` packets from one stream
    /// before round-robin moves to the next. A `batch` of 1 is strict
    /// round-robin.
    pub fn new(batch: usize) -> Self {
        Self {
            last_sends: HashMap::default(),
            queue_states: Vec::default(),
            needs_timeout_before_next_poll: true,
            batch: batch.max(1),
            current: None,
            sent_in_batch: 0,
        }
    }
}

impl Default for NullPacer {
    fn default() -> Self {
        Self::new(1)
    }
}

impl Pacer for NullPacer {
    fn set_pacing_rate(&mut self, _padding_bitrate: Bitrate) {
        // We don't care
    }

    fn set_padding_rate(&mut self, _padding_bitrate: Bitrate) {
        // We don't care
    }
    fn poll_timeout(&self) -> (Option<Instant>, Reason) {
        let time = if self.needs_timeout_before_next_poll {
            self.last_sends.values().min().copied()
        } else {
            None
        };

        (time, Reason::Pacer(PacerReason::Handle))
    }

    fn handle_timeout(
        &mut self,
        _now: Instant,
        iter: impl Iterator<Item = QueueState>,
    ) -> Option<PaddingRequest> {
        self.needs_timeout_before_next_poll = false;
        self.queue_states.clear();
        self.queue_states.extend(iter);

        None
    }

    fn poll_queue(&mut self) -> Option<(MidRid, Option<TwccClusterId>)> {
        // Stick with the current queue until `batch` packets have been
        // drained from it (cache locality when fanning out to many streams).
        if let Some(midrid) = self.current {
            let has_more = self
                .queue_states
                .iter()
                .any(|q| q.midrid == midrid && q.snapshot.packet_count > 0);
            if self.sent_in_batch < self.batch && has_more {
                self.needs_timeout_before_next_poll = true;
                return Some((midrid, None));
            }
            self.current = None;
            self.sent_in_batch = 0;
        }

        let non_empty_queues = self
            .queue_states
            .iter()
            .filter(|q| q.snapshot.packet_count > 0);
        // Pick a queue using round robin, prioritize the least recently sent on queue.
        let to_send_on = non_empty_queues.min_by_key(|q| self.last_sends.get(&q.midrid));

        let result = to_send_on.map(|q| (q.midrid, None));

        if let Some((midrid, _)) = result {
            self.needs_timeout_before_next_poll = true;
            self.current = Some(midrid);
            self.sent_in_batch = 0;
        }

        result
    }

    fn register_send(&mut self, now: Instant, _packet_size: DataSize, from: MidRid) {
        let e = self.last_sends.entry(from).or_insert(now);
        *e = now;
        if self.current == Some(from) {
            self.sent_in_batch += 1;
        }
    }

    fn has_padding_queue(&self) -> bool {
        false
    }
}
