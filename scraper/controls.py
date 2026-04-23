"""
Cross-platform runtime control system.

Three input sources are always active simultaneously:
  1. Command file  — write to command.txt   (works on every OS, including remote/SSH)
  2. Single-key    — P/R/Q/S without Enter  (Windows: msvcrt | Unix: tty+select)
  3. Stdin lines   — type a command + Enter (only when NOT in raw key mode)

Valid commands: pause | resume | status | quit | stop | fresh

Design notes
------------
On Unix, the old code ran both _stdin_loop and _unix_key_loop simultaneously.
_unix_key_loop calls tty.setraw(), which switches stdin to raw (unbuffered)
mode. In raw mode, _stdin_loop's `for line in sys.stdin` never completes a
line read because the newline is not treated as a line delimiter — the two
threads race on the same fd and produce corrupt/dropped commands.

Fix: on Unix, run ONLY the key loop for single-key input. Multi-word commands
are handled exclusively through the command file. On Windows, msvcrt.kbhit()
does not conflict with stdin so both sources can coexist.
"""

from __future__ import annotations

import os
import select
import sys
import threading
import time
from typing import Any

from .utils import beep, elapsed


class ControlState:
    """Shared mutable state for scraper flow control."""
    def __init__(self) -> None:
        self.paused: bool = False
        self.stop:   bool = False
        self.fresh:  bool = False


class ControlHandler:
    """
    Manages all user control inputs and applies them to a ControlState.

    Args:
        state:  The shared ControlState instance.
        ctx:    The shared context dict (for status display).
        config: Full config dict (reads 'files.command_file').
    """

    def __init__(self, state: ControlState, ctx: dict[str, Any], config: dict) -> None:
        self.state     = state
        self.ctx       = ctx
        self._cmd_file = config["files"]["command_file"]
        self._running  = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start all background input threads."""
        self._running = True
        is_windows = sys.platform == "win32"
        if is_windows:
            # Windows: msvcrt kbhit() does not conflict with stdin buffering
            self._thread(self._win_key_loop)
            self._thread(self._stdin_loop)
        else:
            # Unix: tty.setraw() conflicts with line-buffered stdin reads.
            # Run ONLY the raw-key loop; multi-word commands go via command file.
            self._thread(self._unix_key_loop)

    def stop_listening(self) -> None:
        """Signal threads to exit (they are daemon threads)."""
        self._running = False

    # ── Main-thread helpers ───────────────────────────────────────────────────

    def check(self) -> None:
        """Non-blocking check of the command file. Call from the main loop."""
        self._check_file()

    def sleep(self, seconds: float) -> None:
        """
        Interruptible sleep: polls for file commands every 0.1 s.

        Args:
            seconds: Total sleep duration.
        """
        end = time.time() + seconds
        while time.time() < end:
            if self.state.stop:
                return
            self._check_file()
            time.sleep(0.1)

    def wait_while_paused(self) -> None:
        """Block until scraper is no longer paused (or stop is signalled)."""
        while self.state.paused and not self.state.stop:
            self._check_file()
            time.sleep(0.1)

    # ── Command dispatch ──────────────────────────────────────────────────────

    def _apply(self, cmd: str) -> None:
        """Apply a normalised command string to the control state."""
        s   = self.state
        tag = f"\n{elapsed(self.ctx['start'])}"

        if cmd == "pause":
            if not s.paused:
                s.paused = True
                beep("stop")
                print(f"{tag} ⏸  PAUSED — press R or write 'resume' to command.txt")

        elif cmd == "resume":
            if s.paused:
                s.paused = False
                beep("resume")
                print(f"{tag} ▶  RESUMED")

        elif cmd in ("quit", "stop", "q"):
            s.stop = True
            beep("stop")
            print(f"{tag} ⏹  STOPPING — finishing current query...")

        elif cmd == "fresh":
            s.fresh = True
            s.stop  = True
            print(f"{tag} 🔄 FRESH RESET — restart to begin from scratch")

        elif cmd == "status":
            self._print_status()

    def _print_status(self) -> None:
        ctx = self.ctx
        qt  = ctx.get("query_times", [])
        avg = sum(qt[-20:]) / max(len(qt[-20:]), 1) if qt else 0
        rem = ctx.get("total_jobs", 0) - ctx.get("done_jobs", 0)
        mins = int(rem * avg / 60) if avg else 0
        eta  = f"{mins // 60}h{mins % 60:02d}m" if mins >= 60 else f"{mins}m"
        rate = round(
            ctx.get("total_saved", 0) / max((time.time() - ctx["start"]) / 60, 0.01), 1
        )
        print(
            f"\n{elapsed(ctx['start'])}  "
            f"q:{ctx.get('done_jobs', 0)}/{ctx.get('total_jobs', 0)} | "
            f"saved:{ctx.get('total_saved', 0)} | {rate:.1f}/min | ETA:~{eta}\n"
        )
        beep("status")

    # ── Input sources ─────────────────────────────────────────────────────────

    def _check_file(self) -> None:
        """Read and clear the command file atomically."""
        if not os.path.exists(self._cmd_file):
            return
        try:
            with open(self._cmd_file, "r+") as f:
                cmd = f.read().strip().lower()
                f.seek(0)
                f.truncate()
            if cmd:
                self._apply(cmd)
        except Exception:
            pass

    def _stdin_loop(self) -> None:
        """
        Read line-buffered commands from stdin (Windows only).

        Not used on Unix because tty.setraw() in _unix_key_loop switches stdin
        to raw mode, making line-by-line reads unreliable.
        """
        try:
            for line in sys.stdin:
                if not self._running:
                    break
                cmd = line.strip().lower()
                if cmd:
                    self._apply(cmd)
        except Exception:
            pass

    def _win_key_loop(self) -> None:
        """Single-keystroke input on Windows via msvcrt."""
        try:
            import msvcrt
        except ImportError:
            return
        key_map = {"P": "pause", "R": "resume", "Q": "quit", "S": "status"}
        while self._running:
            try:
                if msvcrt.kbhit():
                    key = msvcrt.getch().decode(errors="ignore").upper()
                    # Drain any further pending key bytes (e.g. arrow keys send 2)
                    while msvcrt.kbhit():
                        msvcrt.getch()
                    if key in key_map:
                        self._apply(key_map[key])
                time.sleep(0.05)
            except Exception:
                break

    def _unix_key_loop(self) -> None:
        """
        Single-keystroke input on macOS / Linux via tty + select.

        Switches stdin to raw mode for keypress detection.  This is the SOLE
        stdin consumer on Unix — _stdin_loop is NOT started on Unix to avoid
        the race condition caused by two threads sharing one raw-mode fd.

        To send multi-word commands on Unix, use the command file:
            echo pause  > command.txt
            echo resume > command.txt
            echo stop   > command.txt
        """
        try:
            import termios
            import tty
        except ImportError:
            return

        fd  = sys.stdin.fileno()
        try:
            old = termios.tcgetattr(fd)
        except Exception:
            return  # Not a real tty (e.g. piped stdin in CI) — skip silently

        key_map = {"P": "pause", "R": "resume", "Q": "quit", "S": "status"}
        try:
            tty.setraw(fd)
            while self._running:
                if select.select([sys.stdin], [], [], 0.1)[0]:
                    key = sys.stdin.read(1).upper()
                    if key in key_map:
                        self._apply(key_map[key])
        except Exception:
            pass
        finally:
            # Always restore terminal settings, even on crash
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
            except Exception:
                pass

    # ── Helper ────────────────────────────────────────────────────────────────

    def _thread(self, target) -> None:
        threading.Thread(target=target, daemon=True).start()
