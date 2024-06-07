use std::{
    error::Error,
    io,
    ops::ControlFlow,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use crossterm::event::KeyCode;
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols,
    terminal::{Frame, Terminal, Viewport},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Gauge, Paragraph, Widget},
    TerminalOptions,
};

use crate::tracing::Message;

const LOGO: &str = "\
╔═══╗╔╗ ╔╗╔═══╗╔╗ ╔╗╔═══╗╔═══╗
║╔═╗║║║ ║║║╔═╗║║║ ║║║╔══╝║╔═╗║
║╚═╝║║║ ║║║╚══╗║╚═╝║║╚══╗║╚═╝║
║╔╗╔╝║║ ║║╚══╗║║╔═╗║║╔══╝║╔╗╔╝
║║║╚╗║╚═╝║║╚═╝║║║ ║║║╚══╗║║║╚╗
╚╝╚═╝╚═══╝╚═══╝╚╝ ╚╝╚═══╝╚╝╚═╝\
";

const BUNNY: &str = "  //
 ('>
 /rr
*\\))_";

const INFO_CELL_SIZE: usize = 13;

#[derive(Debug)]
enum Event {
    Input(crossterm::event::KeyEvent),
    Tick,
    Resize,
    Message(crate::tracing::Message),
}

#[derive(Debug, Default)]
struct ExecutorState {
    id: usize,
    name: String,
    users: u64,
    max_users: u64,
    iterations: u64,
    total_iteration: Option<u64>,
    duration: Duration,
    total_duration: Option<Duration>,
    stage: Option<usize>,
    stage_duration: Option<Duration>,
    stages: Option<usize>,
    task_min_time: Duration,
    task_max_time: Duration,
    task_total_time: Duration,
}

pub struct Scenario {
    id: usize,
    name: String,
    execs: Vec<ExecutorState>,
}

impl Scenario {
    pub fn new_from_scenario(id: usize, scenario: &crate::logical::Scenario<'_>) -> Self {
        let name = scenario.label.to_string();
        let execs = scenario
            .execution_provider
            .iter()
            .enumerate()
            .map(|(index, exec)| ExecutorState {
                id: index,
                name: exec.label().to_string(),
                ..Default::default()
            })
            .collect();

        Self { id, name, execs }
    }

    fn exec_names(&self) -> impl Iterator<Item = &str> {
        self.execs.iter().map(|x| &*x.name)
    }
}

pub struct App {
    current_scenario: usize,
    current_exec: usize,
    scenarios: Vec<Scenario>,
}

impl App {
    pub fn new<'a, T>(scenarios: T) -> Self
    where
        T: IntoIterator<Item = &'a crate::logical::Scenario<'a>>,
    {
        let scenarios = scenarios
            .into_iter()
            .enumerate()
            .map(|(id, scenario)| Scenario::new_from_scenario(id, scenario))
            .collect();
        Self {
            current_scenario: 0,
            current_exec: 0,
            scenarios,
        }
    }

    fn current_scenario(&self) -> &Scenario {
        &self.scenarios[self.current_scenario]
    }

    fn current_scenario_mut(&mut self) -> &mut Scenario {
        &mut self.scenarios[self.current_scenario]
    }

    fn current_exec(&self) -> &ExecutorState {
        &self.scenarios[self.current_scenario].execs[self.current_exec]
    }
}

impl App {
    pub fn run(
        self,
        mut tracing_messages: crate::Receiver<crate::tracing::Message>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        crossterm::terminal::enable_raw_mode()?;
        let stdout = io::stdout();
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::with_options(
            backend,
            TerminalOptions {
                viewport: Viewport::Inline(17),
            },
        )?;

        let (tx, rx) = mpsc::channel();

        input_handling(tx.clone());

        thread::spawn(move || {
            while let Some(message) = tracing_messages.blocking_recv() {
                let _ = tx.send(Event::Message(message));
            }
        });

        thread::scope(|s| {
            let handler = s.spawn(|| run_app(&mut terminal, self, rx));
            handler.join().unwrap()
        })?;

        let size = terminal.get_frame().size();
        terminal.set_cursor(size.width, size.height + size.y + 1)?;
        crossterm::terminal::disable_raw_mode()?;

        Ok(())
    }
}

fn input_handling(tx: mpsc::Sender<Event>) -> thread::JoinHandle<()> {
    let tick_rate = Duration::from_millis(200);
    thread::spawn(move || {
        let mut last_tick = Instant::now();
        loop {
            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
            if crossterm::event::poll(timeout).unwrap() {
                match crossterm::event::read().unwrap() {
                    crossterm::event::Event::Key(key) => tx.send(Event::Input(key)).unwrap(),
                    crossterm::event::Event::Resize(_, _) => tx.send(Event::Resize).unwrap(),
                    _ => {}
                };
            }
            if last_tick.elapsed() >= tick_rate {
                if tx.send(Event::Tick).is_err() {
                    break;
                }
                last_tick = Instant::now();
            }
        }
    })
}

fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    mut app: App,
    rx: mpsc::Receiver<Event>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut events: Vec<Event> = Vec::new();
    'a: loop {
        // Catch early if the receiver is ded.
        let event = rx.recv()?;
        events.push(event);
        // consume all events
        rx.try_iter().for_each(|x| events.push(x));
        for event in &events {
            match event {
                Event::Input(event) => match event.code {
                    KeyCode::Char('c')
                        if event.modifiers == crossterm::event::KeyModifiers::CONTROL =>
                    {
                        break 'a;
                    }
                    KeyCode::Up => app.current_exec = app.current_exec.saturating_sub(1),
                    KeyCode::Down => {
                        app.current_exec =
                            (app.current_exec + 1).min(app.current_scenario().execs.len() - 1)
                    }
                    _ => (),
                },
                Event::Resize => {
                    terminal.autoresize()?;
                }
                Event::Tick => {
                    terminal.draw(|f| ui(f, &app))?;
                }
                Event::Message(message) => {
                    if let ControlFlow::Break(res) = handle_message(&mut app, message, terminal) {
                        res?;
                        break 'a;
                    }
                }
            }
        }
        events.clear();
    }
    Ok(())
}

fn handle_message<B: Backend>(
    app: &mut App,
    message: &Message,
    terminal: &mut Terminal<B>,
) -> ControlFlow<Result<(), Box<dyn Error + Send + Sync>>, ()> {
    match message {
        crate::tracing::Message::ScenarioChanged { scenario_id } => {
            app.current_scenario = *scenario_id;
            assert_eq!(app.current_scenario().id, *scenario_id)
        }
        crate::tracing::Message::End => {
            // redraw for the last time
            if let Err(err) = terminal.draw(|f| ui(f, app)) {
                return ControlFlow::Break(Err(Box::new(err)));
            }
            return ControlFlow::Break(Ok(()));
        }
        crate::tracing::Message::TerminatedError { err } => {
            let mut text = Text::from(err.as_str());
            if let Some(line) = text.lines.first_mut() {
                line.spans
                    .insert(0, Span::raw("Termination Err: ").bold().red())
            }

            if let Err(err) = terminal
                .insert_before(text.height() as u16, |buf| {
                    Paragraph::new(text).render(buf.area, buf);
                })
                .and_then(|_| terminal.draw(|f| ui(f, app)))
            {
                return ControlFlow::Break(Err(Box::new(err)));
            }

            return ControlFlow::Break(Ok(()));
        }

        crate::tracing::Message::TaskTime {
            execution_id: id,
            start_time,
            end_time,
            events,
            ..
        } => {
            if let Some(exec) = app
                .current_scenario_mut()
                .execs
                .iter_mut()
                .find(|x| x.id == *id)
            {
                let duration = end_time.saturating_duration_since(*start_time);
                exec.iterations += 1;
                exec.task_max_time = exec.task_max_time.max(duration);
                if exec.task_min_time == Duration::ZERO {
                    exec.task_min_time = duration;
                } else {
                    exec.task_min_time = exec.task_min_time.min(duration);
                }
                exec.task_total_time += duration;
            }

            for err in events
                .iter()
                .filter(|x| x.name == "error")
                .flat_map(|x| x.values.iter().filter(|x| x.0 == "err").map(|(_, v)| v))
            {
                let text = Text::from(err.to_string());
                // redraw for the last time
                let _ = terminal.insert_before(text.height() as u16, |buf| {
                    Paragraph::new(text).render(buf.area, buf);
                });
            }
        }
        crate::tracing::Message::ExecutorUpdate {
            id,
            users,
            max_users,
            total_iteration,
            duration,
            total_duration,
            stage,
            stages,
            stage_duration,
        } => {
            if let Some(exec) = app
                .current_scenario_mut()
                .execs
                .iter_mut()
                .find(|x| x.id == *id)
            {
                exec.users = *users;
                exec.max_users = *max_users;
                exec.duration = *duration;
                exec.total_duration = *total_duration;
                exec.total_iteration = *total_iteration;
                exec.stage = *stage;
                exec.stages = *stages;
                exec.stage_duration = *stage_duration
            }
        }
    };
    ControlFlow::Continue(())
}

fn ui(f: &mut Frame, app: &App) {
    let current_scenario = app.current_scenario();
    let area = f.size();

    let scenario_text = Text::from(vec![Line::from(vec![
        "Scenario - ".to_string().bold(),
        current_scenario.name.to_string().into(),
    ])]);

    let mut executors_text = Text::from(Line::from("Executors: ".to_string().bold()));
    for (index, exec) in current_scenario.exec_names().enumerate() {
        let mut line = Line::from_iter([
            if index == app.current_exec {
                Span::from(symbols::DOT).bold()
            } else {
                Span::from(symbols::DOT)
            },
            Span::from(" "),
            Span::raw(exec),
        ]);

        if index == app.current_exec {
            line = line.light_green();
        }

        executors_text.push_line(line)
    }

    let current_exec = app.current_exec();
    let average_time = current_exec
        .task_total_time
        .checked_div(current_exec.iterations as u32)
        .unwrap_or_default();
    let iteration_per_sec = current_exec.iterations as f64 / current_exec.duration.as_secs_f64();
    let max_time = current_exec.task_max_time;
    let min_time = current_exec.task_min_time;

    // No margins here. Margins are applied by children of the main area
    let [left_area, other_info_area] =
        Layout::horizontal([Constraint::Length(34), Constraint::Min(0)]).areas(area);

    // Draw borders
    f.render_widget(Block::bordered().borders(Borders::RIGHT), left_area);

    // Left Area
    let [logo_area, scenario_area, executors_area] = Layout::vertical([
        Constraint::Length(7),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .vertical_margin(1)
    .horizontal_margin(2)
    .areas(left_area);

    let bunny_text = Text::from(BUNNY);
    let bunny_area = Rect::new(
        executors_area.x + executors_area.width - bunny_text.width() as u16,
        executors_area.y + executors_area.height - bunny_text.height() as u16,
        bunny_text.width() as u16,
        bunny_text.height() as u16,
    );

    if (executors_area.width as usize).saturating_sub(executors_text.width()) >= bunny_text.width()
        || (executors_area.height as usize).saturating_sub(executors_text.height())
            >= bunny_text.height()
    {
        f.render_widget(bunny_text, bunny_area);
    }

    f.render_widget(Paragraph::new(LOGO), logo_area);
    f.render_widget(Block::bordered().borders(Borders::BOTTOM), logo_area);
    f.render_widget(scenario_text, scenario_area);
    f.render_widget(executors_text, executors_area);

    let total_users_formatted = current_exec.users.to_string();
    let total_max_users_formatted = current_exec.max_users.to_string();
    let average_time_formatted = format!("{:.2?}", average_time);
    let max_time_formatted = format!("{:.2?}", max_time);
    let min_time_formatted = format!("{:.2?}", min_time);
    let total_iterations_completed_formatted = current_exec.iterations.to_string();
    let iteration_per_sec_formatted = format!("{:.2} iter/sec", iteration_per_sec);

    let mut info_render = vec![
        ("users", Line::from_iter(value_span(&total_users_formatted))),
        (
            "max_users",
            Line::from_iter(value_span(&total_max_users_formatted)),
        ),
        (
            "iteration_time",
            Line::from_iter(
                key_value_span("avg", &average_time_formatted)
                    .into_iter()
                    .chain(key_value_span("max", &max_time_formatted))
                    .chain(key_value_span("min", &min_time_formatted)),
            ),
        ),
        (
            "iterations",
            Line::from_iter(
                key_value_span("total", &total_iterations_completed_formatted)
                    .into_iter()
                    .chain(value_span(&iteration_per_sec_formatted)),
            ),
        ),
    ];

    let stages_formatted = current_exec.stages.map(|x| x.to_string());
    let stage_formatted = current_exec.stage.map(|x| x.to_string());
    let stage_duration_formatted = current_exec
        .stage_duration
        .map(|duration| format!("{:.2?}", duration));

    if let Some(ref stages) = stages_formatted {
        let mut line = Line::default();
        line.spans.extend(key_value_span("total", stages));

        if let Some((stage, duration)) = stage_formatted
            .as_ref()
            .zip(stage_duration_formatted.as_ref())
        {
            value_span(stage)
                .into_iter()
                .rev()
                .for_each(|x| line.spans.insert(0, x));
            line.spans.extend(key_value_span("duration", duration));
        }
        info_render.insert(0, ("current_stage", line))
    }

    let key_size = info_render.iter().map(|(k, _)| k.len()).max().unwrap() + 3;
    let [mut progress_bar_area, other_info_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)])
            .margin(2)
            .spacing(1)
            .horizontal_margin(2)
            .areas(other_info_area);

    progress_bar_area.width = progress_bar_area.width.min(60);

    let progress = if let Some(total_duration) = current_exec.total_duration {
        let duration = &current_exec.duration;
        Gauge::default()
            .label(format!("{duration:?}/{total_duration:?}"))
            .ratio(duration.as_secs_f64() / total_duration.as_secs_f64())
    } else if let Some(total_iteration) = current_exec.total_iteration {
        let iteration = current_exec.iterations;
        Gauge::default()
            .label(format!("{iteration}/{total_iteration}"))
            .ratio(iteration as f64 / total_iteration as f64)
    } else {
        Gauge::default().label("?/???")
    }
    .gauge_style(Style::default().fg(Color::Green).bg(Color::Gray));

    f.render_widget(progress, progress_bar_area);

    let other_info = Layout::vertical(Constraint::from_lengths(
        std::iter::repeat(1).take(info_render.len()),
    ))
    .spacing(1)
    .split(other_info_area);

    for (i, (key, mut info)) in info_render.into_iter().enumerate() {
        let mut padded_key = format!("{:.<width$}", key, width = key_size);
        padded_key.push(':');
        info.spans.insert(0, Span::raw(padded_key));
        info.spans.insert(1, Span::raw(" "));

        f.render_widget(info, other_info[i]);
    }
}

fn padding(n: usize) -> String {
    String::from_iter(std::iter::repeat(' ').take(n))
}

fn key_value_span<'a>(key: &'a str, value: &'a str) -> [Span<'a>; 4] {
    [
        Span::raw(key).green(),
        Span::raw("=").green(),
        Span::raw(value),
        Span::raw(padding(
            INFO_CELL_SIZE
                .saturating_sub(1 + key.len() + value.len())
                .max(1),
        )),
    ]
}

fn value_span(value: &str) -> [Span<'_>; 2] {
    [
        Span::raw(value).light_blue(),
        Span::raw(padding(INFO_CELL_SIZE.saturating_sub(value.len()).max(1))),
    ]
}
