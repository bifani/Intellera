def check_version():
    """
    Check package versions.
    """
    import subprocess
    from platform import python_version
    import numpy
    import pandas
    import sklearn

    print()
    print("Check Versions")
    print()
    print(str(subprocess.check_output(["jupyter", "--version"]), "utf-8"))
    print("{:16} : {:10}".format("python", python_version()))
    for pkg in [pandas, numpy, sklearn]:
        print("{:16} : {:10}".format(pkg.__name__, pkg.__version__))
    print()


def set_path():
    """
    Set repo path.
    """
    import sys
    from pathlib import Path

    git_repo_path = Path().cwd().parents[0]
    git_repo_name = git_repo_path.name

    print()
    print(f"Repo name: {git_repo_name}")
    print(f"Repo path: {git_repo_path}")
    print()

    if git_repo_path.is_dir() and str(git_repo_path) not in sys.path:
        sys.path.append(str(git_repo_path))


def set_modin(engine: str = "dask"):
    """
    Set modin.
    """
    import modin.config as modin_cfg

    # import unidist.config as unidist_cfg
    from distributed import Client

    _ = Client()

    print()
    print(f"Modine engine: {engine}")
    print()

    # modin_cfg.Engine.put("ray")  # Modin will use Ray
    # modin_cfg.Engine.put("dask")  # Modin will use Dask
    # modin_cfg.Engine.put("unidist") # Modin will use Unidist
    modin_cfg.Engine.put(engine)
    # if engine == "unidist":
    #    unidist_cfg.Backend.put("mpi") # Unidist will use MPI backend
    modin_cfg.StorageFormat.put("pandas")


if __name__ == "__main__":
    # check_version()
    set_path()
    # set_modin()
