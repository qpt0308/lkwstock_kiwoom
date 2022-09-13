from line_notify import LineNotify
ACCESS_TOKEN = "BVr3BHma0sD27laDqGHqfbr4Xk6U4cfVZ5PDHYj9ikq"

def send_message(mes):
    notify = LineNotify(ACCESS_TOKEN)
    notify.send(mes)