import StudiKasus2

if __name__ == "__main__":
    case = StudiKasus2.StudiKasus2('localhost', '3306', 'root', '')
    text = open("username.csv", "r")
    text = ''.join([i for i in text]) \
        .replace(";", ",")
    x = open("username.csv","w")
    x.writelines(text)
    x.close()
    
    df = case.import_csv("username.csv")
    print(case.create_db('midtermse'))
    print(case.create_table('midtermse', 'username', df))
    print(case.load_data('midtermse', 'username'))
    print("did it")


