# SoftwareEngineering
Steps to run Python Virtual Environment on Windows and get all the requirements installed inside it and send it to github:
1. Go to your IDE (In my case, VS code) terminal and open your project directory.
2. Run "python -m venv venv" in the terminal to create a new virtual environemnt. After this, you should see a new folder called "venv" created inside your project directory
3. After this point, you want to activate the virtual environment. You need to write "venv\Scripts\activate" on the terminal. If there is no error pops up, You should see "(venv)" keyword before your directory path. It means that your virtual environemnt has successfully activated. But, if you get the result like this :
![image](https://user-images.githubusercontent.com/77273824/157022394-e7263cac-5da6-4969-b375-b6f7dcf1fc52.png)
you need to continue to step 5.
4. Open Windows PowerShell and run it as administrator.
5. Go to your project directory.
6. Type "Set-ExecutionPolicy RemoteSigned".
7. Type "Y". This command is used to change the Execution Policy which protects you from untrusted scripts. You should deactivate it later after you are done activating your virtual environment.
8. Go back to your IDE terminal and write "venv\Scripts\activate" to activate the virtual environment.
9. You should see "(venv)" keyword before your directory path. It means that your virtual environemnt has successfully activated.
10. Remember, after you are done activating the virtual environment, don't forget to restrict the Execution Policy back to default. To do this, go back to your PowerShell and write "Set-ExecutionPolicy Restricted" and choose "Y".
11. To acquire all the requirements installed in the virtual environment, you need to type "freeze > requirement.txt" on your virtual environment project terminal. You should see a new file called "requirements.txt" appear on your project folder. You are done.
12. Now, you want to push it to your github. First, you need to 