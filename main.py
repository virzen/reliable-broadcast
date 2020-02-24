import sys
import random
from mpi4py import MPI

CHANCE_OF_FAILURE = 20

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

def best_effort_broadcast(data, tag):
  for process in range(1, size):
    comm.send(data, dest = process, tag = tag)


# ACTIVE BROADCAST STUFF

delivered_actively = set()

def was_delivered_actively(msg):
  return frozenset(msg.items()) in delivered_actively

def deliver_actively(msg):
  delivered_actively.add(frozenset(msg.items()))


# PASSIVE BROADCAST STUFF

delivered_passively = set()

def was_delivered_passively(msg):
  return frozenset(msg.items()) in delivered_passively

def deliver_passively(msg):
  delivered_passively.add(frozenset(msg.items()))

# TODO: make work for any `size`
correct = dict([(i, True) for i in range(1, size)])

def mark_incorrect(rank_of_incorrect):
  correct[rank_of_incorrect] = False

def is_correct(tested_rank):
  return correct[tested_rank]

fromi = dict([(i, set()) for i in range(1, size)])

def save_delivered_passively(msg, origin):
  hashableMsg = frozenset(msg.items())
  fromi[origin].add((origin, hashableMsg))

def get_saved_from(source):
  return [dict(msg) for (origin, msg) in fromi[source]]


# "MAIN"

debug("Gotowy")

state = RUNNING


if rank == 0:
  while True:
    input = get_input()

    if input != None:
      receiver, tag, data = input
      msgOut = create_message(data)
      pcktOut = create_packet(msgOut, rank)
      comm.send(pcktOut, dest = receiver, tag = tag)

else:
  def die():
    debug("Umieram...")

    for process in range(1, size):
      msgOut = create_message("")
      pcktOut = create_packet(msgOut, rank)
      comm.send(pcktOut, dest = process, tag = FAILURE)

    global state
    state = FINISHED

  def best_effort_broadcast_with_dying(data, tag):
    for process in range(1, size):
      if random.randint(0, 100) < CHANCE_OF_FAILURE:
        die()
        break
      else:
        comm.send(data, dest = process, tag = tag)


  while state != FINISHED:
    status = MPI.Status()
    pcktIn = comm.recv(status=status)
    tag = status.tag
    source = status.source

    # debug(pcktIn)

    msgIn = pcktIn['data']
    data = msgIn['data']
    origin = pcktIn['origin']


    if tag == START_ACTIVE:
      debug("Rozpoczynam aktywny broadcast '{}'".format(data))

      msgOut = create_message(data)
      pcktOut = create_packet(msgOut, rank)
      deliver_actively(msgOut)
      best_effort_broadcast_with_dying(pcktOut, ACTIVE)


    elif tag == ACTIVE:
      if not was_delivered_actively(msgIn):
        debug("DOSTARCZAM aktywny broadcast od {} z danymi '{}' zainicjowany przez {}".format(source, data, origin))

        deliver_actively(msgIn)

        for process in range(1, size):
          comm.send(pcktIn, dest = process, tag = ACTIVE)

      else:
        debug("IGNORUJE aktywny broadcast od {} z danymi '{}' zainicjowany przez {}".format(source, data, origin))



    elif tag == START_PASSIVE:
      debug("Rozpoczynam pasywny broadcast '{}'".format(data))

      msgOut = create_message(data)
      pcktOut = create_packet(msgOut, rank)

      best_effort_broadcast_with_dying(pcktOut, PASSIVE)


    elif tag == PASSIVE:
      if not was_delivered_passively(msgIn):
        debug("DOSTARCZAM pasywny broadcast od {} z danymi '{}' zainicjowany przez {}".format(source, data, origin))
        deliver_passively(msgIn)
        save_delivered_passively(msgIn, origin)

        if not is_correct(source):
          best_effort_broadcast(pcktIn, PASSIVE)
      else:
        debug("IGNORUJE pasywny broadcast od {} z danymi '{}' zainicjowany przez {}".format(source, data, origin))


    elif tag == FAILURE:
      debug("{} ulegl awarii".format(source))

      mark_incorrect(source)

      for msg in get_saved_from(source):
        debug("ROZGLASZAM pasywny broadcast '{}' zainicjowany przez {}".format(msg['data'], source))
        pcktOut = create_packet(msg, source)
        best_effort_broadcast(pcktOut, PASSIVE)


    elif tag == FAIL:
      die()
