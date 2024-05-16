class User(object):
    def __init__(self, username, birth_year, gender):
        self.username = username
        self.birth_year = birth_year
        # 0: male, 1: female
        self.gender = gender


def user_to_dict(user, ctx):
    return dict(
        username=user.username,
        birth_year=user.birth_year,
        gender=user.gender
    )


def dict_to_user(obj, ctx):
    if obj is None:
        return None

    return User(
        obj["username"],
        obj["birth_year"],
        obj["gender"]
    )
