use std::{
    error::Error,
    io,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Alignment, Constraint, Layout, Margin, Rect},
    style::Stylize,
    terminal::{Frame, Terminal, Viewport},
    text::{Line, Span, Text},
    widgets::{block, Block, Borders, Paragraph},
    TerminalOptions,
};

const LOGO: &str = "\
╔═══╗╔╗ ╔╗╔═══╗╔╗ ╔╗╔═══╗╔═══╗
║╔═╗║║║ ║║║╔═╗║║║ ║║║╔══╝║╔═╗║
║╚═╝║║║ ║║║╚══╗║╚═╝║║╚══╗║╚═╝║
║╔╗╔╝║║ ║║╚══╗║║╔═╗║║╔══╝║╔╗╔╝
║║║╚╗║╚═╝║║╚═╝║║║ ║║║╚══╗║║║╚╗
╚╝╚═╝╚═══╝╚═══╝╚╝ ╚╝╚═══╝╚╝╚═╝
";

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
    pub fn new_from_scenario(scenario: &crate::logical::Scenario<'_>) -> Self {
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
            viewport: Viewport::Inline(20),
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
    let currrent_scenario = &app.scenarios[app.current_scenario];
    let executor_names = currrent_scenario
        .execs
        .iter()
        .map(|x| x.name.as_str())
        .collect::<Vec<_>>();
    let iterations = currrent_scenario
        .execs
        .iter()
        .map(|x| x.iterations)
        .sum::<u64>();

    let area = f.size();

    let scenario_text = Text::from(vec![Line::from(vec![
        "Scenario - ".to_string().bold(),
        currrent_scenario.name.to_string().into(),
    ])]);
    let executor_title = Line::from("Executors: ".to_string().bold());
    let mut executors_text = Text::from(executor_title);
    for exec in executor_names {
        executors_text.push_line(Line::from_iter([Span::from("* ").bold(), Span::raw(exec)]))
    }
    let iteration_text = Text::from(iterations.to_string());
    let average_time = currrent_scenario.execs[0].task_total_time.as_secs_f64()
        / currrent_scenario.execs[0].iterations as f64;
    let max_time = currrent_scenario.execs[0].task_max_time.as_secs_f64();

    // No margins here. Margins are applied by children of the main area
    let [left_area, other_info] =
        Layout::horizontal([Constraint::Length(34), Constraint::Min(0)]).areas(area);

    // Draw borders
    f.render_widget(Block::bordered(), left_area);
    f.render_widget(
        Block::bordered().borders(Borders::TOP | Borders::RIGHT | Borders::BOTTOM),
        other_info,
    );

    // Left Area
    let [logo_area, scenario_area, executors_area] = Layout::vertical([
        Constraint::Length(7),
        Constraint::Length(3),
        Constraint::Min(0),
    ])
    .vertical_margin(1)
    .horizontal_margin(2)
    .areas(left_area);
    let scenario_area = scenario_area.inner(&Margin::new(0, 1));

    f.render_widget(Paragraph::new(LOGO), logo_area);
    f.render_widget(scenario_text, scenario_area);
    f.render_widget(executors_text, executors_area);

    let other_areas = Layout::vertical(Constraint::from_lengths([1, 1, 1, 1, 1]))
        .margin(1)
        .split(other_info);

    f.render_widget(
        Paragraph::new(format!("vus: {}", currrent_scenario.execs[0].vus)),
        other_areas[0],
    );
    f.render_widget(
        Paragraph::new(format!("max_vus: {}", currrent_scenario.execs[0].max_vus)),
        other_areas[1],
    );

    f.render_widget(
        Paragraph::new(format!("average_time: {}", average_time)),
        other_areas[2],
    );
    f.render_widget(
        Paragraph::new(format!("max_time: {}", max_time)),
        other_areas[3],
    );
    f.render_widget(iteration_text, other_areas[4]);
}
