# Measuring oversubscription

`density.py` answers "how many agents can share a worker" with numbers taken
from the control plane rather than from a slide.

It has two halves because the question has two halves. How long an agent
actually holds a worker is a measurement, and what a fleet of them needs is a
projection from that measurement. Keeping them apart makes it obvious which
part of an answer is observed and which part is arithmetic.

## Measure

Every `ResumeActor` and `SuspendActor` lands in the ate-api-server log with a
timestamp and an actor name, so a wake is the gap between the two.

```
kubectl -n ate-system logs -l app=ate-api-server --tail=-1 \
  | demo/measure/density.py measure --atespace openclaw-demo
```

You get occupancy per wake, mean and peak concurrency, achieved density and a
duty cycle.

**Capture while the workload runs.** The log is dominated by `ListActors`,
which anything watching the fleet calls every couple of seconds, so the window
that still holds your resumes is short. Reading the buffer after the fact
mostly returns suspends whose matching resume has already scrolled away. They
are reported as unpaired and skipped rather than guessed at, so a run with a
high unpaired count is a truncated log and not a result. Stream it instead:

```
kubectl -n ate-system logs -l app=ate-api-server -f --since=1s > /tmp/ateapi.log &
# ... drive the workload ...
kill %1
demo/measure/density.py measure --atespace openclaw-demo < /tmp/ateapi.log
```

**Occupancy has to come from a workload that parks itself.** A burst-woken
actor never idle-suspends, because the idle clock only follows conversations
the gateway drove, so its interval ends whenever you get round to suspending it
by hand. Measuring a burst tells you how long you waited. A real conversation
turn is the workload to sample: the gateway wakes the actor, the turn runs, and
the idle timeout parks it without anyone intervening.

## Model

Feed the occupancy in and describe the schedule.

```
demo/measure/density.py model --occupancy 10.5 --actors 1000 --period 15m
```

```
Mean worker demand    11.67 workers
Average density       85.7:1

  aligned (all at :00)   1000 workers
  jittered over the period
    P99 peak             29 workers
    achieved density at P99 peak: 34.5:1
```

The average is the part worth distrusting. It is exactly `period / occupancy`,
so it climbs as far as you like by making the cron rarer, and on its own it
says how idle the agents are rather than how well the platform packs them.

The peak is the part you buy. Aligned, every agent fires at the same instant
and the pool has to be the whole fleet or everything queues behind one wake.
Jittered across the period, the same workload fits in a couple of dozen
workers. That gap is the finding, and closing it is a scheduling decision
rather than something the platform can do on your behalf.

## Where the numbers in the example came from

`--occupancy 10.5` is the demo cluster: about 6.0s from inbound message to
reply, then 4.5s to idle-detect and write the checkpoint. Re-measure it for any
fleet you are sizing, because it scales with what the agent is carrying and a
heavier agent takes longer to snapshot.
