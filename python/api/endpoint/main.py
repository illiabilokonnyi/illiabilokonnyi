import sys

import script


def start_local():
    try:
        script.start()
    except:
        return 1
    else:
        return 0


def main():
    try:
        return script.get_app()
    except:
        return 1


if __name__ == "__main__":
    sys.exit(start_local())
