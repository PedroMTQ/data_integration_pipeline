from dataclasses import dataclass, field


# TODO you'd want to have a connnection to prometheus or OTEL
@dataclass
class Metrics:
    success: int = field(default=0)
    failures: int = field(default=0)
    total: int = field(default=0)

    def log_result(self, is_success: bool):
        self.total += 1
        if is_success:
            self.success += 1
        else:
            self.failures += 1

    def __repr__(self):
        return f'Total: {self.total}, Success: {self.success}, Failures: {self.failures}'
