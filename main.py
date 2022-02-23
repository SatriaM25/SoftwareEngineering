import StudiKasus2

if __name__ == "__main__":
    data = StudiKasus2.StudiKasus2("localhost", "3306", "root", "")
    df = data.import_csv("username.csv")
    
    print(df)
