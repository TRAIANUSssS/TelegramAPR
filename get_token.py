import pickle


def get_token():
    return pickle.load(open(f"bot_token.pkl", "rb"))