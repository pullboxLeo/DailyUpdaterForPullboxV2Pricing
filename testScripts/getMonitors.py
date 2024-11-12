from screeninfo import get_monitors

def print_all_monitors():
    monitors = get_monitors()
    
    print(f"Found {len(monitors)} monitors:")
    for i, monitor in enumerate(monitors):
        print(f"\nMonitor {i}:")
        print(f"  Width: {monitor.width}")
        print(f"  Height: {monitor.height}")
        print(f"  Position: x={monitor.x}, y={monitor.y}")
        print(f"  Primary: {monitor.is_primary}")
        print(f"  Name: {monitor.name}")

if __name__ == "__main__":
    print_all_monitors()