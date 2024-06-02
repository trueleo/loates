use std::{
    error::Error,
    fmt::Write,
    io,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Layout, Rect},
    style::Stylize,
    terminal::{Frame, Terminal, Viewport},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph},
    TerminalOptions,
};

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

const INFO_CELL_SIZE: usize = 15;

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

    fn exec_names(&self) -> impl Iterator<Item = &str> {
        self.execs.iter().map(|x| &*x.name)
    }

    fn total_iterations_completed(&self) -> u64 {
        self.execs.iter().map(|x| x.iterations).sum()
    }

    fn total_vus(&self) -> u64 {
        self.execs.iter().map(|x| x.vus).sum()
    }

    fn total_max_vus(&self) -> u64 {
        self.execs.iter().map(|x| x.max_vus).sum()
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

impl App {
    fn current_scenario(&self) -> &Scenario {
        &self.scenarios[self.current_scenario]
    }
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
    let currrent_scenario = app.current_scenario();
    let area = f.size();

    let scenario_text = Text::from(vec![Line::from(vec![
        "Scenario - ".to_string().bold(),
        currrent_scenario.name.to_string().into(),
    ])]);
    let executor_title = Line::from("Executors: ".to_string().bold());
    let mut executors_text = Text::from(executor_title);
    for exec in currrent_scenario.exec_names() {
        executors_text.push_line(Line::from_iter([Span::from("* ").bold(), Span::raw(exec)]))
    }
    let average_time = currrent_scenario.execs[0]
        .task_total_time
        .checked_div(currrent_scenario.execs[0].iterations as u32)
        .unwrap_or_default();
    let max_time = currrent_scenario.execs[0].task_max_time;

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

    let total_vus_formatted = currrent_scenario.total_vus().to_string();
    let total_max_vus_formatted = currrent_scenario.total_max_vus().to_string();
    let average_time_formatted = format!("{:?}", average_time);
    let max_time_formatted = format!("{:?}", max_time);
    let total_iterations_completed_formattted =
        currrent_scenario.total_iterations_completed().to_string();

    let info_render = [
        ("vus", Line::from_iter(value_span(&total_vus_formatted))),
        (
            "max_vus",
            Line::from_iter(value_span(&total_max_vus_formatted)),
        ),
        (
            "iteration_time",
            Line::from_iter(
                key_value_span("avg", &average_time_formatted)
                    .into_iter()
                    .chain(key_value_span("max", &max_time_formatted)),
            ),
        ),
        (
            "iterations",
            Line::from_iter(key_value_span(
                "total",
                &total_iterations_completed_formattted,
            )),
        ),
    ];

    let key_size = info_render.iter().map(|(k, _)| k.len()).max().unwrap() + 3;
    let other_info = Layout::vertical(Constraint::from_lengths(
        std::iter::repeat(1).take(info_render.len()),
    ))
    .vertical_margin(1)
    .horizontal_margin(2)
    .spacing(1)
    .split(other_info);

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
        Span::raw(key).bold(),
        Span::raw("=").bold(),
        Span::raw(value).bold().blue(),
        Span::raw(padding(INFO_CELL_SIZE - 1 - key.len() - value.len())),
    ]
}

fn value_span(value: &str) -> [Span<'_>; 2] {
    [
        Span::raw(value).bold().blue(),
        Span::raw(padding(INFO_CELL_SIZE - value.len())),
    ]
}
