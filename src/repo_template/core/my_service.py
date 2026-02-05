from repo_template.io.logger import logger

def my_mock_function(*args, **kwargs):
    logger.info(f'My args: {args}, My kwargs: {kwargs}')

if __name__ == '__main__':
    my_mock_function('Hello', 'World!', my_key='value')