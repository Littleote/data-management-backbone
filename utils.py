# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os

class Path():
    """
    Allows to change working directory with contexts

    Parameters
    ----------
    directory : str, optional
        Directory to set after initialization. The default is None.

    Returns
    -------
    None.

    """
    def __init__(self, directory: str = None):
        self.base = os.getcwd()
        self.directories = []
        self.move(directory)

    def __enter__(self):
        self._acces_last()

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def move(self, path: str):
        """
        Move working directory to another path

        Parameters
        ----------
        path : str
            Next path.

        Returns
        -------
        None.

        """
        if path is not None:
            self.directories.append(path)
        self._acces_last()

    def undo(self):
        """
        Return working directory to previous path

        Returns
        -------
        None.

        """
        if len(self.directories) > 0:
            self.directories.pop()
        self._acces_last()


    def reset(self):
        """
        Return working directory to original path

        Returns
        -------
        None.

        """
        self.directories = []
        os.chdir(self.base)

    def _acces_last(self):
        if len(self.directories) > 0:
            os.chdir(self.directories[-1])
        else:
            os.chdir(self.base)


def get_zones():
    """
    Get an instance of the list of zones

    Returns
    -------
    list
        Zones of the program.

    """
    return [
        "landing",
        "formatted",
        "trusted",
        "exploitation",
        "sandbox",
        "feature_generation",
        "models",
        "dataset_info"
    ]


def confirm(question: str | None, default: bool | None = True,
            *, force_confirmation: bool = False,
            labels: list[str] = None) -> bool | None:
    """
    Asks user confirmation for a yes or no choice

    Parameters
    ----------
    question : str | None
        Question to ask the user for confirmation.
    default : bool | None, optional
        Default result in case of no or wrong response. The default is True.
    force_confirmation : bool, optional
        Ensure the answer is true or false. When True, default is ignored. The default is False.
    labels : list[str], optional
        labels for confirmation answer. The default is ["Y", "N"].

    Returns
    -------
    bool | None
        Confirmation result or None if failed.

    """
    if labels is None:
        labels = ["Y", "N"]
    if force_confirmation:
        default = None
    options = {
        True: "[{true}]/{false}\n",
        None: "{true}/{false}\n",
        False: "{true}/[{false}]\n",
    }
    while True:
        if question is not None:
            print(question)
        choice = input(options[default].format(true=labels[0], false=labels[1]))
        positive_match = len([None for a, b in zip(labels[0].lower(), choice.lower()) if a == b])
        negative_match = len([None for a, b in zip(labels[1].lower(), choice.lower()) if a == b])
        if positive_match > negative_match:
            return True
        if negative_match > positive_match:
            return False
        if force_confirmation:
            print(f"Answer must be either '{labels[0]}' or '{labels[1]}', not '{choice}'")
            continue
        break
    return default


def select(name: str, options: list):
    """
    Asks user to select an option among a set

    Parameters
    ----------
    name : str
        Name of the options presented to the user.
    options : list[T]
        List of options to present the user.

    Returns
    -------
    T or None
        Selected option or None if failed.

    """
    if len(options) > 1:
        # Choose if there are multiple
        print(f"Available {name}s:")
        for i, option in enumerate(options):
            print(f"   {i + 1}.- {option}")
        print()
        option = input(f"Select a {name}'s index: ")
        try:
            option = int(option)
            assert 0 < option <= len(options)
        except (AssertionError, ValueError):
            print("Invalid index")
            return None
        return options[option - 1]

    if len(options) == 1:
        # Confirm if there's only one
        if confirm(f"Only {name} available is {options[0]}, procced?"):
            return options[0]
    # Cancel if not
    return None
