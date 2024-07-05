pub mod ui;

use std::{
    error::Error,
    io,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use ratatui::{
    backend::{Backend, CrosstermBackend},
    crossterm::{self, event::KeyCode},
    style::Stylize,
    terminal::{Terminal, Viewport},
    text::{Span, Text},
    widgets::{Paragraph, Widget},
    TerminalOptions,
};

use crate::tracing::message::Message;

use ui::ui;

#[derive(Debug)]
enum Event {
    Input(crossterm::event::KeyEvent),
    Tick,
    Resize,
    Message(Message),
}

#[derive(Debug, Default)]
struct TuiState {
    current_exec_selected: usize,
    execs_len: usize,
}

pub fn run(
    app: Arc<Mutex<super::App>>,
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

    let _app = app.clone();
    thread::spawn(move || {
        while let Some(message) = tracing_messages.blocking_recv() {
            if matches!(
                message,
                Message::End
                    | Message::Error { .. }
                    | Message::TerminatedError { .. }
                    | Message::ScenarioChanged { .. }
            ) {
                let _ = tx.send(Event::Message(message.clone()));
            }
            _app.lock().unwrap().handle_message(message);
        }
    });

    run_app(&mut terminal, app, rx)?;

    let size = terminal.get_frame().size();
    terminal.set_cursor(size.width, size.height + size.y + 1)?;
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
    app: Arc<Mutex<super::App>>,
    rx: mpsc::Receiver<Event>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut tui_state = TuiState {
        current_exec_selected: 0,
        execs_len: app.lock().unwrap().current_scenario().execs.len(),
    };

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
                    KeyCode::Up => {
                        tui_state.current_exec_selected =
                            tui_state.current_exec_selected.saturating_sub(1)
                    }
                    KeyCode::Down => {
                        tui_state.current_exec_selected =
                            (tui_state.current_exec_selected + 1).min(tui_state.execs_len - 1)
                    }
                    _ => (),
                },
                Event::Resize => {
                    terminal.autoresize()?;
                }
                Event::Tick => {
                    terminal.draw(|f| ui(f, &app, &tui_state))?;
                }
                Event::Message(message) => {
                    match message {
                        Message::End => {
                            // redraw for the last time
                            terminal.draw(|f| ui(f, &app, &tui_state))?;
                            break 'a;
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
                                .and_then(|_| terminal.draw(|f| ui(f, &app, &tui_state)))
                            {
                                return Err(Box::new(err));
                            }
                        }
                        Message::Error { err } => {
                            let text = Text::from(err.to_string());
                            // redraw for the last time
                            let _ = terminal.insert_before(text.height() as u16, |buf| {
                                Paragraph::new(text).render(buf.area, buf);
                            });
                        }
                        Message::ScenarioChanged { .. } => {
                            let app = &app.lock().unwrap();
                            tui_state.current_exec_selected = 0;
                            tui_state.execs_len = app.current_scenario().execs.len()
                        }
                        _ => (),
                    }
                }
            }
        }
    }
    Ok(())
}
