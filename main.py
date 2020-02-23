import sys
from mpi4py import MPI

# MPI INIT

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


# MESSAGES

ACTIVE = 1
PASSIVE = 2
FAILURE = 3
START_ACTIVE = 4
START_PASSIVE = 5
FAIL = 6

# STATES

RUNNING = 1
FINISHED = 2


# LIB

def debug(msg):
  print("{}: {}".format(rank, msg))

def get_input():
  input = sys.stdin.readline().strip()
  elements = input.split(" ")

  if len(elements) == 3:
    [receiver, tag, data] = elements
    return int(receiver), int(tag), data
  else:
    return None

def create_packet(data, origin):
  return { 'data': data, 'origin': origin }

def make_create_message():
  next_index = 0

  def f(data):
    nonlocal next_index
    message = { 'data': data, 'id': "{}-{}".format(rank, next_index)}
    next_index += 1
    return message

  return f

create_message = make_create_message()

def create_packet(data, origin):
  message = create_message(data)
  return { 'message': message, 'origin': origin }


# ACTIVE BROADCAST STUFF

# ...

# PASSIVE BROADCAST STUFF

# ...


# "MAIN"

debug("Gotowy")

if rank == 0:
  while True:
    input = get_input()

    if input != None:
      receiver, tag, data = input
      packetOut = create_packet(data, rank)
      comm.send(packetOut, dest = receiver, tag = tag)

else:
  state = RUNNING

  while state != FINISHED:
    status = MPI.Status()
    packetIn = comm.recv(status=status)
    tag = status.tag
    source = status.source
    data = packetIn['message']['data']

    if tag == START_ACTIVE:
      debug("Rozpoczynam aktywny broadcast '{}'".format(data))
      for process in range(1, size):
        packetOut = create_packet(data, rank)
        comm.send(packetOut, dest = process, tag = ACTIVE)

    elif tag == ACTIVE:
      debug("Dostalem aktywny broadcast od {} z danymi '{}'".format(source, data))

    elif tag == START_PASSIVE:
      debug("Rozpoczynam pasywny broadcast")

    elif tag == PASSIVE:
      debug("Dostalem pasywny broadcast od {}".format(source))

    elif tag == FAILURE:
      debug("{} ulegl awarii".format(source))

    elif tag == FAIL:
      debug("Umieram...")

      packetOut = create_packet("", rank)
      for process in range(1, size):
        comm.send(packetOut, dest = process, tag = FAILURE)

      state = FINISHED
