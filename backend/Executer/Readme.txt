I split the query_bot file into the executor file and the interpreter file under the interpreter folder
to match the file structure, since they should be seperated. The interactive main loop is not included in either of these
so tests have to be done on the website. I included a copy below. -kon



# 7. Interactive query loop
if __name__ == "__main__":
    print("Welcome to HoopQuery! Type 'quit' to exit.")
    while True:
        user_inp = input("\nAsk a question about NBA data: ")
        if user_inp.lower() in ["quit", "exit"]:
            break
        natural_language_to_sql(user_inp)


def run_query(question: str):
    return natural_language_to_sql(question)