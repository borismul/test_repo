
class UnexpectedStatusCodeError(Exception):

    def __init__(self, error_code):
        self.error_code = error_code
        self.message = f'Unexpected status code {self.error_code}. Expected 200, 304 or 403'
        super().__init__(self.message)