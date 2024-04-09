from bevy import get_repository


def di_repo():
    container = get_repository()
    return container
