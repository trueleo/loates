use std::{
    error::Error,
    io,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use futures::StreamExt;
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Alignment, Constraint, Layout},
    style::Stylize,
    terminal::{Frame, Terminal, Viewport},
    text::{Line, Text},
    widgets::{block, Block, Paragraph},
    TerminalOptions,
};

use crate::logical;

enum Event {
    Input(crossterm::event::KeyEvent),
    Tick,
    Resize,
    Message(crate::tracing::Message),
}

#[derive(Debug, Default)]
struct ExecutorState {
    name: String,
    vus: u64,
    max_vus: u64,
    iterations: u64,
    task_min_time: Duration,
    task_max_time: Duration,
    task_total_time: Duration,
}

pub struct Scenario {
    name: String,
    execs: Vec<ExecutorState>,
}

impl Scenario {
    pub fn new_from_scenario(scenario: &logical::Scenario<'_>) -> Self {
        let name = scenario.name.clone();
        let execs = scenario
            .execution_provider
            .iter()
            .map(|exec| ExecutorState {
                name: exec.name().to_string(),
                ..Default::default()
            })
            .collect();

        Self { name, execs }
    }

    fn update(&mut self, message: &crate::tracing::Message) {
        match message {
            crate::tracing::Message::TaskTime {
                exec_name,
                duration,
                ..
            } => {
                if let Some(exec) = self.execs.iter_mut().find(|x| *x.name == **exec_name) {
                    exec.task_max_time = exec.task_max_time.max(*duration);
                    exec.task_min_time = exec.task_min_time.min(*duration);
                    exec.task_total_time += *duration;
                }
            }
            crate::tracing::Message::ExecutorUpdate {
                name,
                vus,
                max_vus,
                iterations,
            } => {
                if let Some(exec) = self.execs.iter_mut().find(|x| x.name == name.to_string()) {
                    exec.vus = *vus;
                    exec.max_vus = *max_vus;
                    exec.iterations = *iterations;
                }
            }
            _ => {}
        }
    }
}

struct App {
    current_scenario: usize,
    scenarios: Vec<Scenario>,
}

pub fn run(
    mut tracing_messages: crate::Receiver<crate::tracing::Message>,
    scenarios: Vec<Scenario>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    crossterm::terminal::enable_raw_mode()?;
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Inline(10),
        },
    )?;

    let (tx, rx) = mpsc::channel();

    let app = App {
        current_scenario: 0,
        scenarios,
    };

    input_handling(tx.clone());

    thread::spawn(move || loop {
        match tracing_messages.try_next() {
            Ok(Some(message)) => {
                let _ = tx.send(Event::Message(message));
            }
            Ok(None) => break,
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    });

    thread::scope(|s| {
        let handler = s.spawn(|| run_app(&mut terminal, app, rx));
        handler.join().unwrap()
    })?;

    let size = terminal.get_frame().size();
    terminal.set_cursor(size.width, size.height + size.y)?;
    crossterm::terminal::disable_raw_mode()?;

    Ok(())
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
    let mut redraw = true;
    loop {
        if redraw {
            terminal.draw(|f| ui(f, &app))?;
        }
        redraw = true;

        match rx.recv()? {
            Event::Input(event) => {
                if event.code == crossterm::event::KeyCode::Char('q') {
                    break;
                }
                if event.code == crossterm::event::KeyCode::Char('c')
                    && event.modifiers == crossterm::event::KeyModifiers::CONTROL
                {
                    break;
                }
            }
            Event::Resize => {
                terminal.autoresize()?;
            }
            Event::Tick => {}
            Event::Message(message) => match message {
                crate::tracing::Message::ScenarioChanged { scenario_name } => {
                    app.current_scenario = app
                        .scenarios
                        .iter()
                        .position(|scenario| scenario.name == scenario_name)
                        .unwrap();
                }
                crate::tracing::Message::End => break,
                _ => app.scenarios[app.current_scenario].update(&message),
            },
        };
    }
    Ok(())
}

fn ui(f: &mut Frame, app: &App) {
    let area = f.size();
    let [main_bar, other_info] = Layout::vertical([Constraint::Length(3), Constraint::Min(0)])
        .margin(1)
        .areas(area);
    let [scenario_area, executors_area, iterations_area] =
        Layout::horizontal(Constraint::from_lengths([24, 15, 10]))
            .margin(1)
            .areas(main_bar);
    let other_areas = Layout::vertical(Constraint::from_lengths([1, 1, 1, 1]))
        .horizontal_margin(1)
        .split(other_info);

    let block = Block::bordered().title(block::Title::from(" Rusher ").alignment(Alignment::Left));
    f.render_widget(block, area);

    let currrent_scenario = &app.scenarios[app.current_scenario];
    let scenario = Text::from(vec![Line::from(vec![
        "Scenario ".to_string().bold().white(),
        currrent_scenario.name.to_string().into(),
    ])]);
    let executors = Text::from(
        currrent_scenario
            .execs
            .iter()
            .map(|x| x.name.as_str())
            .collect::<String>(),
    );
    let iterations = currrent_scenario
        .execs
        .iter()
        .map(|x| x.iterations)
        .sum::<u64>();
    let iterations = Text::from(iterations.to_string());
    f.render_widget(scenario, scenario_area);
    f.render_widget(executors, executors_area);
    f.render_widget(iterations, iterations_area);

    f.render_widget(
        Paragraph::new(format!("vus: {}", currrent_scenario.execs[0].vus)),
        other_areas[0],
    );
    f.render_widget(
        Paragraph::new(format!("max_vus: {}", currrent_scenario.execs[0].max_vus)),
        other_areas[1],
    );
    f.render_widget(
        Paragraph::new(format!(
            "average_time: {}",
            currrent_scenario.execs[0].task_total_time.as_secs_f64()
                / currrent_scenario.execs[0].iterations as f64
        )),
        other_areas[2],
    );
    f.render_widget(
        Paragraph::new(format!(
            "max_time: {}",
            currrent_scenario.execs[0].task_max_time.as_secs_f64()
        )),
        other_areas[3],
    );
}
