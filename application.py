# This is a sample Python script.
from src.email_logic.email_sender import EmailSender


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

    email_sender = EmailSender()

    email_sender.send_bulk_emails(r"C:\Users\jeppe\Documents\GitHub\REO\files\test.csv")


    #test

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
