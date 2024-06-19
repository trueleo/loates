use std::{collections::VecDeque, fmt::Debug, sync::Mutex, time::Duration};

use itertools::Itertools;
use ordered_float::OrderedFloat;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Flex, Layout, Margin, Rect},
    style::{Color, Style, Stylize},
    symbols,
    text::{Line, Span, Text},
    widgets::{
        block::Title, Axis, Bar, BarChart, BarGroup, Block, Borders, Chart, Dataset, Gauge,
        GraphType, Padding, Paragraph,
    },
    Frame,
};

use crate::{
    app::{App, ExecutorState},
    tracing::task_event::{
        metrics::{MetricType, MetricValue},
        MetricSetKey,
    },
};

use super::TuiState;

const LOGO: &str = "\
╔═══╗╔╗ ╔╗╔═══╗╔╗ ╔╗╔═══╗╔═══╗
║╔═╗║║║ ║║║╔═╗║║║ ║║║╔══╝║╔═╗║
║╚═╝║║║ ║║║╚══╗║╚═╝║║╚══╗║╚═╝║
║╔╗╔╝║║ ║║╚══╗║║╔═╗║║╔══╝║╔╗╔╝
║║║╚╗║╚═╝║║╚═╝║║║ ║║║╚══╗║║║╚╗
╚╝╚═╝╚═══╝╚═══╝╚╝ ╚╝╚═══╝╚╝╚═╝\
";
const INFO_CELL_SIZE: usize = 13;

struct Size {
    height: u16,
    width: u16,
}

fn logo() -> (Size, fn(&mut Frame, Rect)) {
    let f = |f: &mut Frame, rect: Rect| {
        f.render_widget(Paragraph::new(LOGO), rect);
    };

    (
        Size {
            height: 6,
            width: 7,
        },
        f,
    )
}

fn scenario_text(name: &str) -> (Size, impl FnOnce(&mut Frame, Rect) + '_) {
    let scenario_text = Line::from(vec!["Scenario - ".to_string().bold(), name.into()]);
    let width = scenario_text.width() as u16;
    let f = move |f: &mut Frame, rect: Rect| {
        f.render_widget(scenario_text, rect);
    };

    (Size { height: 1, width }, f)
}

fn executor_text<'a>(
    current_exec: usize,
    exec_names: impl Iterator<Item = String>,
) -> (Size, impl FnOnce(&mut Frame, Rect) + 'a) {
    let mut executors_text = Text::from(Line::from("Executors: ".to_string().bold()));
    for (index, exec) in exec_names.enumerate() {
        let mut line = Line::from_iter([
            if index == current_exec {
                Span::from(symbols::DOT).bold()
            } else {
                Span::from(symbols::DOT)
            },
            Span::from(" "),
            Span::raw(exec),
        ]);

        if index == current_exec {
            line = line.light_green();
        }

        executors_text.push_line(line)
    }

    let width = executors_text.width() as u16;
    let height = executors_text.height() as u16;

    let f = move |f: &mut Frame, rect: Rect| {
        f.render_widget(executors_text, rect);
    };

    (Size { height, width }, f)
}

fn progress_bar(current: &ExecutorState) -> (Size, impl FnOnce(&mut Frame, Rect)) {
    let progress = if let Some(total_duration) = current.total_duration {
        let duration = current.duration();
        Gauge::default()
            .label(format!("{duration:.2?}/{total_duration:.2?}"))
            .ratio((duration.as_secs_f64() / total_duration.as_secs_f64()).min(1f64))
    } else if let Some(total_iteration) = current.total_iteration {
        let iteration = current.iterations;
        Gauge::default()
            .label(format!("{iteration}/{total_iteration}"))
            .ratio((iteration as f64 / total_iteration as f64).min(1f64))
    } else {
        Gauge::default().label("?/???")
    }
    .gauge_style(Style::default().fg(Color::Green).bg(Color::Gray));

    let f = move |f: &mut Frame, rect: Rect| {
        f.render_widget(progress, rect);
    };

    (
        Size {
            height: 1,
            width: 60,
        },
        f,
    )
}

fn other_info(current: &ExecutorState) -> (Size, impl FnOnce(&mut Frame, Rect) + '_) {
    let average_time = current
        .task_total_time
        .checked_div(current.iterations as u32)
        .unwrap_or_default();

    let total_users_formatted = current.users.to_string();
    let total_max_users_formatted = current.max_users.to_string();
    let average_time_formatted = format!("{:.2?}", average_time);
    let max_time_formatted = format!("{:.2?}", current.task_max_time);
    let min_time_formatted = format!("{:.2?}", current.task_min_time);
    let total_iterations_completed_formatted = current.iterations.to_string();
    let iteration_per_sec_formatted = format!(
        "{:.2} iter/sec",
        current.iterations as f64 / current.duration().as_secs_f64()
    );

    let stages_formatted = current.stages.map(|x| x.to_string());
    let stage_formatted = current.stage.map(|x| x.to_string());
    let stage_duration_formatted = current
        .stage_duration
        .map(|duration| format!("{:.2?}", duration));

    let mut info_render = Vec::default();

    if let Some(stages) = stages_formatted {
        let line = if let Some((stage, duration)) = stage_formatted.zip(stage_duration_formatted) {
            Line::from_iter(
                value_span(stage)
                    .into_iter()
                    .chain(key_value_span("total", stages))
                    .chain(key_value_span("duration", duration)),
            )
        } else {
            Line::from_iter(key_value_span("total", stages))
        };
        info_render.push(("current_stage", line))
    }

    info_render.extend([
        ("users", Line::from_iter(value_span(total_users_formatted))),
        (
            "max_users",
            Line::from_iter(value_span(total_max_users_formatted)),
        ),
        (
            "iteration_time",
            Line::from_iter(
                key_value_span("avg", average_time_formatted)
                    .into_iter()
                    .chain(key_value_span("max", max_time_formatted))
                    .chain(key_value_span("min", min_time_formatted)),
            ),
        ),
        (
            "iterations",
            Line::from_iter(
                key_value_span("total", total_iterations_completed_formatted)
                    .into_iter()
                    .chain(value_span(iteration_per_sec_formatted)),
            ),
        ),
    ]);

    let key_size = info_render.iter().map(|(k, _)| k.len()).max().unwrap() + 2;

    let mut paragraph = Text::default();

    for (i, (key, mut info)) in info_render.into_iter().enumerate() {
        if i != 0 {
            paragraph.lines.push(Line::default());
        }
        let padded_key = format!("{:.<width$}:", key, width = key_size);
        info.spans.insert(0, Span::raw(padded_key));
        info.spans.insert(1, Span::raw(" "));
        paragraph.lines.push(info)
    }

    let size = Size {
        height: paragraph.height() as u16,
        width: paragraph.width() as u16,
    };

    let f = move |f: &mut Frame, rect: Rect| {
        f.render_widget(paragraph, rect);
    };

    (size, f)
}

fn render_gauge(key: &MetricSetKey, value: &VecDeque<MetricValue>, f: &mut Frame, area: Rect) {
    let Some(min_value) = value.iter().reduce(|x, y| x.min_gauge(y)) else {
        return;
    };
    let Some(max_value) = value.iter().reduce(|x, y| x.min_gauge(y)) else {
        return;
    };
    let mid_value = min_value.mid(max_value);

    let data_points: Vec<(f64, f64)> = value
        .iter()
        .enumerate()
        .map(|(x, &y)| {
            let y = match y {
                MetricValue::GaugeF64(x) => x,
                MetricValue::GaugeI64(x) => x as f64,
                MetricValue::GaugeU64(x) => x as f64,
                MetricValue::GaugeDuration(x) => x.as_millis() as f64,
                _ => 0.,
            };

            (x as f64, y)
        })
        .collect();

    let data = Dataset::default()
        .name("metrics")
        .marker(symbols::Marker::Braille)
        .graph_type(GraphType::Line)
        .data(&data_points);

    // Create the X axis and define its properties
    let x_axis = Axis::default()
        .bounds([0.0, data_points.len() as f64])
        .labels(vec!["0".into(), data_points.len().to_string().into()]);

    // Create the Y axis and define its properties
    let min = data_points
        .iter()
        .map(|x| OrderedFloat(x.1))
        .min()
        .map(|x| x.0)
        .unwrap_or_default();

    let min = (min - min * 0.1).trunc();

    let max = data_points
        .iter()
        .map(|x| OrderedFloat(x.1))
        .max()
        .map(|x| x.0)
        .unwrap_or(10.);

    let max = (max + max * 0.2).ceil();

    let y_axis = Axis::default()
        .title(
            data_points
                .last()
                .map(|x| x.1.to_string())
                .unwrap_or_default(),
        )
        .bounds([min, max])
        .labels(vec![
            min_value.to_string().into(),
            mid_value.to_string().into(),
            max_value.to_string().into(),
        ]);

    let chart = Chart::new(vec![data])
        .block(
            Block::new()
                .title(title(key))
                .title_alignment(Alignment::Right),
        )
        .x_axis(x_axis)
        .y_axis(y_axis);

    f.render_widget(chart, area)
}

fn render_histogram<'a>(
    key: &MetricSetKey,
    value: impl Iterator<Item = &'a MetricValue>,
    f: &mut Frame,
    area: Rect,
) {
    let value = value.last().unwrap();

    fn bar<'a, T: Debug>(
        name: &'static str,
        value: &'a T,
        max: &'a T,
        norm: fn(&'a T, &'a T) -> u64,
    ) -> Bar<'static> {
        let norm = norm(value, max);
        Bar::default()
            .value(norm)
            .text_value(format!("{:.2?}", value))
            .label(name.into())
    }

    let (bars, sum): (BarGroup, &dyn Debug) = match value {
        MetricValue::Histogram(((p50, p90, p95, p99), sum)) => (
            BarGroup::default().bars(&[
                bar("p50", p50, p99, norm_f64),
                bar("p90", p90, p99, norm_f64),
                bar("p95", p95, p99, norm_f64),
                bar("p99", p99, p99, norm_f64),
            ]),
            sum,
        ),
        MetricValue::DurationHistogram(((p50, p90, p95, p99), sum)) => (
            BarGroup::default().bars(&[
                bar("p50", p50, p99, norm_duration),
                bar("p90", p90, p99, norm_duration),
                bar("p95", p95, p99, norm_duration),
                bar("p99", p99, p99, norm_duration),
            ]),
            sum,
        ),
        _ => unreachable!(),
    };

    let mut title = title(key);
    title
        .content
        .spans
        .extend([Span::raw("sum=").green(), Span::raw(format!("{:.2?}", sum))]);

    let barchart = BarChart::default()
        .block(
            Block::new()
                .title(title)
                .padding(Padding::new(3, 3, 1, 1))
                .title_alignment(Alignment::Left),
        )
        .direction(Direction::Horizontal)
        .bar_width(1)
        .bar_gap(0)
        .bar_style(Style::new().green())
        .value_style(Style::new().black())
        .data(bars)
        .max(100);

    f.render_widget(barchart, area)
}

fn render_counter<'a>(
    key: &MetricSetKey,
    values: impl Iterator<Item = &'a MetricValue>,
    f: &mut Frame,
    rect: Rect,
) {
    let value = values.last().unwrap();
    let MetricValue::Counter(value) = value else {
        unreachable!()
    };

    let mut line = title(key).content;
    line.spans
        .extend([Span::raw(" - "), Span::raw(value.to_string())]);
    line.alignment = Some(Alignment::Left);
    f.render_widget(line, rect);
}

fn render_metrics(metrics: &[(&MetricSetKey, &VecDeque<MetricValue>)], rect: Rect, f: &mut Frame) {
    let layout = Layout::vertical(metrics.iter().map(|(key, _)| match key.metric_type {
        MetricType::Counter => Constraint::Length(2),
        MetricType::Gauge => Constraint::Length(10),
        MetricType::Histogram => Constraint::Length(7),
    }))
    .spacing(1)
    .split(rect);

    for (metric, &rect) in metrics.iter().zip(layout.iter()) {
        let rect = rect.inner(&Margin {
            horizontal: 2,
            vertical: 0,
        });
        match metric.0.metric_type {
            MetricType::Gauge => render_gauge(metric.0, metric.1, f, rect),
            MetricType::Histogram => render_histogram(metric.0, metric.1.iter(), f, rect),
            MetricType::Counter => render_counter(metric.0, metric.1.iter(), f, rect),
        }
    }
}

fn margin(rect: Rect, h: u16, v: u16) -> Rect {
    rect.inner(&Margin {
        horizontal: h,
        vertical: v,
    })
}

fn title(key: &MetricSetKey) -> Title {
    let mut title: Title = Title::from(format!("{}_{} ", key.name, key.metric_type.to_string()));
    for attr in &key.attributes {
        title.content.spans.extend([
            Span::raw(attr.0).green(),
            Span::raw("=").green(),
            Span::raw(attr.1.to_string()),
        ]);
        title.content.push_span(Span::raw(" "));
    }
    title
}

pub(super) fn ui(f: &mut Frame, app: &Mutex<App>, state: &TuiState) {
    let area = f.size();
    let app = app.lock().unwrap();

    let (logo_size, logo_render) = logo();
    let (scenario_size, scenario_render) = scenario_text(&app.current_scenario().name);
    let (executor_size, executor_render) = executor_text(
        state.current_exec_selected,
        app.current_scenario().exec_names(),
    );
    let (progress_size, progress_render) =
        progress_bar(&app.current_scenario().execs[state.current_exec_selected]);
    let (info_size, info_render) =
        other_info(&app.current_scenario().execs[state.current_exec_selected]);

    let left_width = logo_size
        .width
        .max(scenario_size.width)
        .max(executor_size.width)
        .max(progress_size.width)
        .max(info_size.width)
        + 4;

    // No margins here. Margins are applied by children of the main area
    let [left_area, metric_area] =
        Layout::horizontal([Constraint::Length(left_width), Constraint::Min(0)]).areas(area);

    // Draw borders
    f.render_widget(Block::bordered().borders(Borders::RIGHT), left_area);

    let left_height = 1
        + logo_size.height
        + 1
        + scenario_size.height
        + executor_size.height
        + 1
        + progress_size.height
        + 1
        + info_size.height
        + 1;

    if left_height > left_area.height {
        // cant render the whole thing
        f.render_widget(
            Text::raw("Too Small").red().bold().centered(),
            Layout::vertical([Constraint::Length(1)])
                .flex(Flex::Center)
                .split(left_area)[0],
        )
    } else {
        // Left Area
        let [logo_area, scenario_area, executors_area, _, progress_area, _, info_area] =
            Layout::vertical([
                Constraint::Length(logo_size.height + 1),
                Constraint::Length(scenario_size.height),
                Constraint::Length(executor_size.height),
                Constraint::Length(1),
                Constraint::Length(progress_size.height),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .vertical_margin(1)
            .areas(left_area);

        f.render_widget(Block::bordered().borders(Borders::BOTTOM), logo_area);
        f.render_widget(
            Span::raw("┤"),
            Rect {
                x: logo_area.width - 1,
                y: logo_area.height,
                width: 1,
                height: 1,
            },
        );

        logo_render(f, margin(logo_area, 2, 0));
        scenario_render(f, margin(scenario_area, 2, 0));
        progress_render(f, margin(progress_area, 2, 0));
        executor_render(f, margin(executors_area, 2, 0));
        info_render(f, margin(info_area, 2, 0));

        let metric_area = margin(metric_area, 1, 1);
        let metrics = app.current_scenario().execs[state.current_exec_selected]
            .metrics
            .iter()
            .sorted_by_key(|(x, _)| x.name)
            .collect_vec();
        render_metrics(&metrics, metric_area, f)
    }
}

fn padding(n: usize) -> String {
    String::from_iter(std::iter::repeat(' ').take(n))
}

fn key_value_span(key: &'static str, value: String) -> [Span<'static>; 4] {
    let size = 1 + key.len() + value.len();
    [
        Span::raw(key).green(),
        Span::raw("=").green(),
        Span::raw(value),
        Span::raw(padding(INFO_CELL_SIZE.saturating_sub(size).max(1))),
    ]
}

fn value_span(value: String) -> [Span<'static>; 2] {
    let size = value.len();
    [
        Span::raw(value).light_blue(),
        Span::raw(padding(INFO_CELL_SIZE.saturating_sub(size).max(1))),
    ]
}

fn norm_duration(x: &Duration, max: &Duration) -> u64 {
    let x_norm = (x.as_nanos() * 100)
        .checked_div(max.as_nanos())
        .unwrap_or(0);
    x_norm as u64
}

fn norm_f64(x: &f64, max: &f64) -> u64 {
    let x_norm = (x / max) * 100.;
    if x_norm.is_nan() {
        return 0;
    }
    let x_norm: u64 = unsafe { x_norm.to_int_unchecked() };
    x_norm
}
