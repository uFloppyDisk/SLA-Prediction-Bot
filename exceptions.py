"""exceptions.py."""


class HLTVError(Exception):
    """Base Exception Class."""


class NoMatchesFound(HLTVError):
    """Exception for when no matches have been found."""

    def __init__(self, state=1, msg=None):
        match_states = {
            1: "upcoming",
            0: "ongoing",
            -1: "result"
        }

        if msg is None:
            msg = f"No {match_states[state]} matches have been found."

        super().__init__(msg)

        self.state = state
        self.message = msg

    def __str__(self):
        return f"{self.__class__.__name__}({self.message})"
