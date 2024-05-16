class User(object):
    def __init__(self, username, birth_year, gender):
        self.username = username
        self.birth_year = birth_year
        # 0: male, 1: female
        self.gender = gender


def to_dict(user, ctx):
    return dict(
        username=user.username,
        birth_year=user.birth_year,
        gender=user.gender
    )
