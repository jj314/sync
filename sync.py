import argparse
import filecmp
import logging
import shutil
import sys
from pathlib import Path
from time import sleep
import stat


def main():
    args = parse_arguments()
    logger = setup_logging(args.log_path)
    while True:
        synchronize(args.source_path, args.replica_path, logger)
        sleep(args.interval_seconds)


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='sync',
        description='Periodically synchronizes a replica directory with a source directory.')
    parser.add_argument('source_path', type=existing_directory)
    parser.add_argument('replica_path', type=Path)
    parser.add_argument('-i', '--interval_seconds', type=positive_float, default='60')
    parser.add_argument('-l', '--log_path', type=Path, default='./log.txt')
    args = parser.parse_args()
    return args


def existing_directory(path_str):
    path = Path(path_str)
    if not path.is_dir():
        raise ValueError
    return path


def positive_float(interval_str):
    interval = float(interval_str)
    if interval <= 0:
        raise ValueError
    return interval


def setup_logging(log_path):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.FileHandler(log_path))
    logger.addHandler(logging.StreamHandler(sys.stdout))
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    for h in logger.handlers:
        h.setFormatter(formatter)
    return logger


def synchronize(source: Path, replica: Path, logger):
    handle_removals(source, replica, logger)
    handle_copies(source, replica, logger)


def handle_removals(source: Path, replica: Path, logger):
    for p in replica.glob('**/*'):
        relative = p.relative_to(replica)
        in_source = source / relative
        if not in_source.exists():
            logger.info(f'REMOVE {relative}')
            try:
                remove_write_lock(p)
                if p.is_file():
                    p.unlink()
                else:
                    shutil.rmtree(p)
            except PermissionError as e:
                logger.warning(f'Unable to remove {relative}. Reason: {e}')


def handle_copies(source: Path, replica: Path, logger):
    def ignore(directory, content):
        directory = Path(directory)
        directory_replica = replica / (directory.relative_to(source))
        to_be_ignored = []
        for child in content:
            source_child = directory / child
            replica_child = directory_replica / child
            if not replica_child.exists():
                logger.info(f'CREATE {child}')
                continue
            if (source_child.is_file() or replica_child.is_file()) and not filecmp.cmp(source_child, replica_child):
                logger.info(f'COPY {child}')
                remove_write_lock(replica_child)
                continue
            if source_child.is_file():
                to_be_ignored.append(child)
        return to_be_ignored

    shutil.copytree(source, replica, ignore=ignore, dirs_exist_ok=True)


def remove_write_lock(path):
    path.chmod(stat.S_IWRITE)


if __name__ == '__main__':
    main()
