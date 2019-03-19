import os


def hdfs_fn(job, filename):
    """Prepend a filename with the job's HDFS workspace directory path."""
    return os.path.join(job._project.config['hdfs_dir'], job._id, filename)
