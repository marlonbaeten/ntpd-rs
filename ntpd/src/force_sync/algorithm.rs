use std::fmt::Debug;
use std::{collections::HashMap, marker::PhantomData};

use ntp_proto::{
    Measurement, NtpClock, NtpDuration, PollInterval, SourceConfig, SourceController,
    TimeSyncController,
};
use serde::Deserialize;

use crate::daemon::spawn::SourceId;

#[derive(Debug, Clone)]
pub enum Measurements {
    Ntp(Measurement<NtpDuration>),
    Sock(Measurement<()>),
}

impl Measurements {
    fn get_offset(&self) -> NtpDuration {
        match self {
            Measurements::Ntp(measurement) => measurement.offset,
            Measurements::Sock(measurement) => measurement.offset,
        }
    }
}

pub trait WrapMeasurements<D: Debug + Copy + Clone> {
    fn wrap(&self) -> Measurements;
}

impl WrapMeasurements<NtpDuration> for Measurement<NtpDuration> {
    fn wrap(&self) -> Measurements {
        Measurements::Ntp(*self)
    }
}

impl WrapMeasurements<()> for Measurement<()> {
    fn wrap(&self) -> Measurements {
        Measurements::Sock(*self)
    }
}

pub(crate) struct SingleShotController<C> {
    pub(super) clock: C,
    sources: HashMap<SourceId, Measurements>,
    min_agreeing: usize,
}

#[derive(Debug, Copy, Clone, Deserialize)]
pub(crate) struct SingleShotControllerConfig {
    pub expected_sources: usize,
}

pub(crate) struct SingleShotSourceController<D: Debug + Copy + Clone> {
    delay_type: PhantomData<D>,
    min_poll_interval: PollInterval,
    done: bool,
    ignore: bool,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum SingleShotControllerMessage {}

impl<C: NtpClock> SingleShotController<C> {
    const ASSUMED_UNCERTAINTY: NtpDuration = NtpDuration::from_exponent(-1);

    fn try_steer(&self) {
        if self.sources.len() < self.min_agreeing {
            return;
        }

        struct Event {
            offset: NtpDuration,
            count: isize,
        }
        let mut events: Vec<_> = self
            .sources
            .values()
            .flat_map(|m| {
                [
                    Event {
                        offset: m.get_offset() - Self::ASSUMED_UNCERTAINTY,
                        count: 1,
                    },
                    Event {
                        offset: m.get_offset() + Self::ASSUMED_UNCERTAINTY,
                        count: -1,
                    },
                ]
                .into_iter()
            })
            .collect();
        events.sort_by(|a, b| a.offset.cmp(&b.offset));

        let mut peak = 0;
        let mut peak_offset = events[0].offset;
        let mut cur = 0;
        for ev in events {
            cur += ev.count;
            if cur > peak {
                peak = cur;
                peak_offset = ev.offset;
            }
        }

        if peak as usize >= self.min_agreeing {
            let mut sum = 0.0;
            let mut count = 0;
            for source in self.sources.values() {
                if source.get_offset().abs_diff(peak_offset) <= Self::ASSUMED_UNCERTAINTY {
                    count += 1;
                    sum += source.get_offset().to_seconds()
                }
            }

            let avg_offset = NtpDuration::from_seconds(sum / (count as f64));
            self.offer_clock_change(avg_offset);

            #[cfg(not(test))]
            std::process::exit(0);
        }
    }
}

impl<C: NtpClock> TimeSyncController for SingleShotController<C> {
    type Clock = C;
    type SourceId = SourceId;
    type AlgorithmConfig = SingleShotControllerConfig;
    type ControllerMessage = SingleShotControllerMessage;
    type SourceMessage = Measurements;
    type NtpSourceController = SingleShotSourceController<NtpDuration>;
    type OneWaySourceController = SingleShotSourceController<()>;

    fn new(
        clock: Self::Clock,
        synchronization_config: ntp_proto::SynchronizationConfig,
        algorithm_config: Self::AlgorithmConfig,
    ) -> Result<Self, <Self::Clock as ntp_proto::NtpClock>::Error> {
        Ok(SingleShotController {
            clock,
            sources: HashMap::new(),
            min_agreeing: synchronization_config
                .minimum_agreeing_sources
                .max(algorithm_config.expected_sources / 2),
        })
    }

    fn take_control(&mut self) -> Result<(), <Self::Clock as ntp_proto::NtpClock>::Error> {
        //no need for actions
        Ok(())
    }

    fn add_source(
        &mut self,
        _id: Self::SourceId,
        config: SourceConfig,
    ) -> Self::NtpSourceController {
        SingleShotSourceController::<NtpDuration> {
            delay_type: PhantomData,
            min_poll_interval: config.poll_interval_limits.min,
            done: false,
            ignore: false,
        }
    }

    fn add_one_way_source(
        &mut self,
        _id: Self::SourceId,
        config: SourceConfig,
        _measurement_noise_estimate: f64,
        period: Option<f64>,
    ) -> Self::OneWaySourceController {
        SingleShotSourceController::<()> {
            delay_type: PhantomData,
            min_poll_interval: config.poll_interval_limits.min,
            done: false,
            ignore: period.is_some(),
        }
    }

    fn remove_source(&mut self, id: Self::SourceId) {
        self.sources.remove(&id);
    }

    fn source_update(&mut self, id: Self::SourceId, usable: bool) {
        if !usable {
            self.sources.remove(&id);
        }
    }

    fn source_message(
        &mut self,
        id: Self::SourceId,
        message: Self::SourceMessage,
    ) -> ntp_proto::StateUpdate<Self::SourceId, Self::ControllerMessage> {
        self.sources.insert(id, message);
        // TODO, check and update time once we have sufficient sources
        self.try_steer();
        Default::default()
    }

    fn time_update(&mut self) -> ntp_proto::StateUpdate<Self::SourceId, Self::ControllerMessage> {
        // no need for action
        Default::default()
    }
}

impl<D: Debug + Copy + Clone + Send + 'static> SourceController for SingleShotSourceController<D>
where
    Measurement<D>: WrapMeasurements<D>,
{
    type ControllerMessage = SingleShotControllerMessage;
    type MeasurementDelay = D;
    type SourceMessage = Measurements;

    fn handle_message(&mut self, _message: Self::ControllerMessage) {
        //ignore
    }

    fn handle_measurement(
        &mut self,
        measurement: Measurement<Self::MeasurementDelay>,
    ) -> Option<Self::SourceMessage> {
        self.done = true;
        if self.ignore {
            None
        } else {
            Some(measurement.wrap())
        }
    }

    fn desired_poll_interval(&self) -> ntp_proto::PollInterval {
        if self.done {
            PollInterval::NEVER
        } else {
            self.min_poll_interval
        }
    }

    fn observe(&self) -> ntp_proto::ObservableSourceTimedata {
        ntp_proto::ObservableSourceTimedata::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntp_proto::{NtpInstant, NtpLeapIndicator, NtpTimestamp, SynchronizationConfig};

    #[derive(Debug, Clone, Default)]
    struct TestClock;

    impl NtpClock for TestClock {
        type Error = std::convert::Infallible;

        fn now(&self) -> Result<NtpTimestamp, Self::Error> {
            Ok(NtpTimestamp::from_seconds_nanos_since_ntp_era(0, 0))
        }

        fn set_frequency(&self, _freq: f64) -> Result<NtpTimestamp, Self::Error> {
            self.now()
        }

        fn get_frequency(&self) -> Result<f64, Self::Error> {
            Ok(0.0)
        }

        fn step_clock(&self, _offset: NtpDuration) -> Result<NtpTimestamp, Self::Error> {
            self.now()
        }

        fn disable_ntp_algorithm(&self) -> Result<(), Self::Error> {
            Ok(())
        }

        fn error_estimate_update(
            &self,
            _est_error: NtpDuration,
            _max_error: NtpDuration,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn status_update(&self, _leap_status: NtpLeapIndicator) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn measurement(offset: f64) -> Measurement<NtpDuration> {
        Measurement {
            delay: NtpDuration::ZERO,
            offset: NtpDuration::from_seconds(offset),
            localtime: NtpTimestamp::from_seconds_nanos_since_ntp_era(0, 0),
            monotime: NtpInstant::now(),
            stratum: 0,
            root_delay: NtpDuration::ZERO,
            root_dispersion: NtpDuration::ZERO,
            leap: NtpLeapIndicator::NoWarning,
            precision: 0,
        }
    }

    fn controller() -> SingleShotController<TestClock> {
        SingleShotController::new(
            TestClock::default(),
            SynchronizationConfig {
                minimum_agreeing_sources: 2,
                ..Default::default()
            },
            SingleShotControllerConfig {
                expected_sources: 2,
            },
        )
        .unwrap()
    }

    #[test]
    fn does_not_step_without_agreement() {
        super::super::reset_offered_offset();
        let mut ctrl = controller();

        ctrl.source_message(SourceId::new(), Measurements::Ntp(measurement(0.0)));
        ctrl.source_message(SourceId::new(), Measurements::Ntp(measurement(10.0)));

        assert!(super::super::offered_offset().is_none());
    }

    #[test]
    fn steps_when_sources_agree() {
        super::super::reset_offered_offset();
        let mut ctrl = controller();

        ctrl.source_message(SourceId::new(), Measurements::Ntp(measurement(1.0)));
        ctrl.source_message(SourceId::new(), Measurements::Ntp(measurement(1.2)));

        let offset = super::super::offered_offset().expect("offset expected");
        assert!((offset.to_seconds() - 1.1).abs() < 1e-6);
    }
}
