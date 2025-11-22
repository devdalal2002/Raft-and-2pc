import subprocess
import sys

def main():
	result = subprocess.run([sys.executable, "tests/run_all_tests.py"])
	sys.exit(result.returncode)

if __name__ == "__main__":
	main()
