from metis_fn import singleton

class InitState(singleton.Singleton):

    state = []

    def add_state(self, thing):
        self.state.append(thing)

    def clear_state(self):
        self.state = []
