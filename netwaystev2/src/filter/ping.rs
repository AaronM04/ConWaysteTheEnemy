/*
 * Copyright (C) 2020 The Conwayste Developers
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of  MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#![allow(dead_code)] // Because this file is pub for server.rs. TODO: Refactor server into crate

use std::collections::VecDeque;
use std::time::Instant;

use rand::random;
use serde::{Deserialize, Serialize};

/// This indicates the number of samples needed by the latency filter to calculate a statistically
/// meaningful average.
const LATENCY_FILTER_DEPTH: usize = 12;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct PingPong {
    pub nonce: u64,
}

impl PingPong {
    pub fn ping() -> PingPong {
        PingPong { nonce: random::<u64>() }
    }

    pub fn pong(nonce: u64) -> PingPong {
        PingPong { nonce }
    }
}

/// A moving-average filter used to level out the latencies calculated from network request/response times.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct LatencyFilter {
    pub average_latency_ms: Option<u64>,
    running_sum:            u64,
    history:                VecDeque<u64>,
    start_timestamp:        Instant,
    in_progress:            bool,
}

impl LatencyFilter {
    pub fn new() -> LatencyFilter {
        LatencyFilter {
            average_latency_ms: None,
            running_sum:        0,
            history:            VecDeque::with_capacity(LATENCY_FILTER_DEPTH),
            start_timestamp:    Instant::now(),
            in_progress:        false,
        }
    }

    #[deny(unused_variables)]
    pub fn reset(&mut self) {
        let Self {
            ref mut average_latency_ms,
            ref mut running_sum,
            ref mut history,
            ref mut start_timestamp,
            ref mut in_progress,
        } = *self;

        *average_latency_ms = None;
        *running_sum = 0;
        history.clear();
        *start_timestamp = Instant::now();
        *in_progress = false;
    }

    pub fn start(&mut self) {
        self.start_timestamp = Instant::now();
        self.in_progress = true;
    }

    pub fn update(&mut self) {
        if !self.in_progress {
            error!("[FILTER] The LatencyFilter's start() was not called so a duration cannot be computed.");
            let elapsed = self.start_timestamp.elapsed();
            error!(
                "LatencyFilter.start_timestamp snapshot was {}.{:06} seconds ago.",
                elapsed.as_secs(),
                elapsed.subsec_micros()
            );
        }

        let latency = Instant::now().duration_since(self.start_timestamp);
        let latency_ms = latency.as_millis() as u64;

        self.running_sum += latency_ms;
        self.history.push_back(latency_ms);

        // Wait for the filter to be populated
        if self.history.len() > LATENCY_FILTER_DEPTH {
            // unwraps safe b/c of length check
            let oldest = self.history.pop_front().unwrap();
            self.running_sum -= oldest;

            // average round-trip time
            let average_rtt_ms = (self.running_sum as f64 / LATENCY_FILTER_DEPTH as f64) as u64;
            let average_latency_ms = average_rtt_ms / 2;

            self.average_latency_ms = Some(average_latency_ms);
        }

        self.in_progress = false;
    }

    #[cfg(test)]
    fn set_start_time(&mut self, ms_in_past: u64) {
        use std::time::Duration;

        self.in_progress = true;
        let opt_past_timestamp = Instant::now().checked_sub(Duration::from_millis(ms_in_past));
        if let Some(past_timestamp) = opt_past_timestamp {
            self.start_timestamp = past_timestamp;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_filter_under_filled_does_not_set_latency() {
        let mut pf = LatencyFilter::new();

        (0..LATENCY_FILTER_DEPTH).into_iter().for_each(|_| {
            pf.set_start_time(500);
            pf.update();
        });

        assert_eq!(pf.average_latency_ms, None);
    }

    #[test]
    fn test_latency_filter_filled_sets_latency() {
        let mut pf = LatencyFilter::new();

        (0..=LATENCY_FILTER_DEPTH).into_iter().for_each(|_| {
            pf.set_start_time(500);
            pf.update();
        });

        assert_eq!(pf.average_latency_ms, Some(250));

        // Perform an additional 12 for shiggles
        (0..=LATENCY_FILTER_DEPTH).into_iter().for_each(|_| {
            pf.set_start_time(500);
            pf.update();
        });

        assert_eq!(pf.average_latency_ms, Some(250));
    }

    #[test]
    fn test_latency_filter_filled_sets_latency_with_varying_pings() {
        let mut pf = LatencyFilter::new();

        (0..=LATENCY_FILTER_DEPTH * 100).step_by(100).into_iter().for_each(|i| {
            pf.set_start_time(i as u64);
            pf.update();
        });

        assert_eq!(pf.average_latency_ms, Some(325));
    }
}
