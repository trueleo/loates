pub mod ui;

use std::{
    collections::{HashMap, VecDeque},
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
    style::Stylize,
    terminal::{Terminal, Viewport},
    text::{Span, Text},
    widgets::{Paragraph, Widget},
    TerminalOptions,
};

use crate::tracing::{
    message::Message,
    task_event::{metrics::MetricValue, MetricSetKey},
};

use ui::ui;

#[derive(Debug)]
enum Event {
    Input(crossterm::event::KeyEvent),
    Tick,
    Resize,
    Message(Message),
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
    metrics: HashMap<MetricSetKey, VecDeque<MetricValue>>,
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
        mut tracing_messages: crate::Receiver<Message>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        crossterm::terminal::enable_raw_mode()?;
        let stdout = io::stdout();
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::with_options(
            backend,
            TerminalOptions {
                viewport: Viewport::Fullscreen,
            },
        )?;
        let _ = terminal.clear();

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
        for event in events.drain(..) {
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
    }
    Ok(())
}

fn handle_message<B: Backend>(
    app: &mut App,
    message: Message,
    terminal: &mut Terminal<B>,
) -> ControlFlow<Result<(), Box<dyn Error + Send + Sync>>, ()> {
    match message {
        Message::ScenarioChanged { scenario_id } => {
            app.current_scenario = scenario_id;
            assert_eq!(app.current_scenario().id, scenario_id)
        }
        Message::End => {
            // redraw for the last time
            if let Err(err) = terminal.draw(|f| ui(f, app)) {
                return ControlFlow::Break(Err(Box::new(err)));
            }
            return ControlFlow::Break(Ok(()));
        }
        Message::TerminatedError { err } => {
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
        Message::Error { err } => {
            let text = Text::from(err.to_string());
            // redraw for the last time
            let _ = terminal.insert_before(text.height() as u16, |buf| {
                Paragraph::new(text).render(buf.area, buf);
            });
        }
        Message::TaskTime {
            execution_id: id,
            start_time,
            end_time,
            ..
        } => {
            if let Some(exec) = app
                .current_scenario_mut()
                .execs
                .iter_mut()
                .find(|x| x.id == id)
            {
                let duration = end_time.saturating_duration_since(start_time);
                exec.iterations += 1;
                exec.task_max_time = exec.task_max_time.max(duration);
                if exec.task_min_time == Duration::ZERO {
                    exec.task_min_time = duration;
                } else {
                    exec.task_min_time = exec.task_min_time.min(duration);
                }
                exec.task_total_time += duration;
            }
        }
        Message::ExecutorUpdate {
            id,
            users,
            max_users,
            total_iteration,
            duration,
            total_duration,
            stage,
            stages,
            stage_duration,
            metrics,
        } => {
            if let Some(exec) = app
                .current_scenario_mut()
                .execs
                .iter_mut()
                .find(|x| x.id == id)
            {
                exec.users = users;
                exec.max_users = max_users;
                exec.duration = duration;
                exec.total_duration = total_duration;
                exec.total_iteration = total_iteration;
                exec.stage = stage;
                exec.stages = stages;
                exec.stage_duration = stage_duration;
                metrics.into_iter().for_each(|(key, value)| {
                    let entry = exec.metrics.entry(key).or_default();
                    if entry.len() >= 20 {
                        entry.pop_front();
                    }
                    entry.push_back(value)
                });
            }
        }
    };
    ControlFlow::Continue(())
}
