use inc_stats::Percentiles;
use itertools::Itertools;
use std::time::Instant;

#[derive(Default)]
pub struct Durations {
    put: Duration,
    del: Option<Duration>,
}

#[derive(Default)]
struct Duration {
    run_percentiles: Percentiles<f64>,
    total_percentiles: Percentiles<f64>,
}

impl Durations {
    fn del(&mut self) -> &mut Duration {
        self.del.get_or_insert(Default::default())
    }

    pub fn measure_put<T>(&mut self, f: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed();

        self.put.run_percentiles.add(elapsed.as_secs_f64());
        self.put.total_percentiles.add(elapsed.as_secs_f64());

        result
    }

    pub fn finish_put_run(&mut self) -> String {
        let result = calculate_percentiles(&self.put.run_percentiles);
        self.put.run_percentiles = Default::default();
        result
    }

    pub fn measure_del<T>(&mut self, f: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed();

        self.del().run_percentiles.add(elapsed.as_secs_f64());
        self.del().total_percentiles.add(elapsed.as_secs_f64());

        result
    }

    pub fn finish_del_run(&mut self) -> String {
        let result = calculate_percentiles(&self.del().run_percentiles);
        self.del().run_percentiles = Default::default();
        result
    }

    pub fn finish(mut self) -> (String, String) {
        (
            calculate_percentiles(&self.put.total_percentiles),
            calculate_percentiles(&self.del().total_percentiles),
        )
    }
}

fn calculate_percentiles(percentiles: &Percentiles<f64>) -> String {
    [0.5, 0.9, 1.0]
        .iter()
        .map(|percentile| {
            format!(
                "{percentile}: {}",
                percentiles
                    .percentile(percentile)
                    .unwrap()
                    .map(std::time::Duration::from_secs_f64)
                    .map(|duration| format!("{duration:?}"))
                    .unwrap_or("N/A".to_string())
            )
        })
        .join(", ")
}
