class Settings():

    def __init__(self) -> None:

        self.key = {
            "AWS_ACCESS_KEY_ID":"XXXXXXXXXXXXXXXXXXXX",
            "AWS_SECRET_ACCESS_KEY":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "IS_SANDBOX":True
        }

        self.task = {
            'TASK_FILENAME': "task_positive.xml", # xml format only
            'TASK_TITLE': "Shoplifting Detection",
            'DESCRIPTION': "Answer if shoplifting is taking place in the movie. (~1 minutes)",
            'KEYWORDS': "image, detection", # comma separated
            'REWARD': "0.02",
            'MAX_ASSIGNMENTS': 30,
            'LIFETIME_IN_SECONDS': 300,
            'ASSIGNMENT_DURATION_IN_SECONDS': 300,
            'APPROVAL_DELAY_IN_SECONDS': 0
        }

        self.record = {
            'STATUS_CHECK_NUM':10,  # Depending on LIFETIME_IN_SECONDS
            'STATUS_CHECK_INTERVAL':10  # default:10
        }

        return None