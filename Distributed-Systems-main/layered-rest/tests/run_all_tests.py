import os
import subprocess
import sys

def main():
    # Get the absolute path to the tests directory (this file's directory)
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    test_files = [
        "test_add_remove.py",
        "test_vote.py",
        "test_sync.py",
        "test_metadata.py",
        "test_history.py",
    ]
    all_passed = True
    for test in test_files:
        test_path = os.path.join(tests_dir, test)
        print(f"\nRunning {os.path.basename(test_path)}...")
        result = subprocess.run([sys.executable, test_path])
        if result.returncode != 0:
            print(f"{os.path.basename(test_path)}: FAIL")
            all_passed = False
        else:
            print(f"{os.path.basename(test_path)}: PASS")
    if all_passed:
        print("\nALL TESTS PASSED")
    else:
        print("\nSOME TESTS FAILED")

if __name__ == "__main__":
    main()
