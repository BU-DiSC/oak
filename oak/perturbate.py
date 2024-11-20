import random

from oak.types import Predicate, Query


class Perturbator:
    def __init__(self, query: Query, distance: float) -> None:
        self.query = query
        self.distance = distance

    def gen_noise(self, starting):
        # We should generate different types of noise over different types, like
        # datetime or strings
        return starting + random.uniform(0, self.distance)

    def gen_perturbed_predicate(self):
        predicates = []
        for predicate in self.query.predicates:
            perturbed_value = self.gen_noise(predicate.value)
            predicates.append(Predicate(name=predicate.name, value=perturbed_value))

        return predicates
