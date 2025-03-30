import asyncio
import time
import sys
import tty
import termios
import os
import uuid

from container_engine import ContainerEngine

def setup_terminal():
    """Set up terminal for raw input mode."""
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    tty.setraw(sys.stdin.fileno())
    return old_settings

def restore_terminal(old_settings):
    """Restore terminal to original settings."""
    fd = sys.stdin.fileno()
    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

async def read_output(receive_data, detach_event):
    """Task to continuously read output until detached."""
    try:
        async for chunk in receive_data():
            if chunk:  # Only print non-empty chunks
                sys.stdout.write(chunk.decode())
                sys.stdout.flush()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"\n❌ Error reading output: {e}")
    finally:
        if not detach_event.is_set():
            detach_event.set()

async def read_input(send_data, detach_event):
    """Task to read raw input from terminal and send to container."""
    old_settings = setup_terminal()
    loop = asyncio.get_event_loop()
    
    def handle_input():
        try:
            char = sys.stdin.read(1)
            if char == '\x03':  # Ctrl+C
                detach_event.set()
                loop.remove_reader(sys.stdin.fileno())
            else:
                # For escape sequences, read the rest of the sequence
                if char == '\x1b':
                    rest = sys.stdin.read(1)
                    if rest == '[':  # CSI sequence
                        # Read until we get a letter
                        while True:
                            next_char = sys.stdin.read(1)
                            if next_char.isalpha():
                                asyncio.create_task(send_data((char + rest + next_char).encode()))
                                break
                    else:
                        # Not a CSI sequence, send both chars
                        asyncio.create_task(send_data((char + rest).encode()))
                else:
                    asyncio.create_task(send_data(char.encode()))
        except Exception as e:
            print(f"\n❌ Error reading input: {e}")
            detach_event.set()
            loop.remove_reader(sys.stdin.fileno())

    try:
        loop.add_reader(sys.stdin.fileno(), handle_input)
        while not detach_event.is_set():
            await asyncio.sleep(0.1)
    finally:
        loop.remove_reader(sys.stdin.fileno())
        restore_terminal(old_settings)

async def main():
    engine = ContainerEngine()

    # Start the root container with a shell
    container = await engine.start_container(
        image='alpine',
        name=f'interactive_test-{str(uuid.uuid4())[:8]}',
        interactive=True,
        args=['/bin/sh']  # Start with a shell
    )
    print(f"✅ Started interactive sandbox {container.id}")

    # Attach to the container
    detach_event, send_data, receive_data = await engine.attach_container(container.id)

    try:
        # Start the reading and input tasks
        read_task = asyncio.create_task(read_output(receive_data, detach_event))
        input_task = asyncio.create_task(read_input(send_data, detach_event))
        
        # Start vi
        await send_data(b"vi\n")
        
        # Wait for tasks to complete
        await asyncio.gather(read_task, input_task)
            
    finally:
        # Detach when done
        detach_event.set()
        try:
            await asyncio.gather(read_task, input_task)
        except asyncio.CancelledError:
            pass
        await container.stop()
        print("\n✅ Container stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")