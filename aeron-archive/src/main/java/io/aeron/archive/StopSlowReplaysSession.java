package io.aeron.archive;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.Iterator;
import java.util.Map;

class StopSlowReplaysSession implements Session {
    private final long correlationId;
    private final ControlSession controlSession;
    private final Iterator<Map.Entry<Long, ReplaySession>> replayIterator;
    private final long recordingId;
    private final long stopPosition;
    private boolean isDone = false;

    StopSlowReplaysSession(
            final long correlationId,
            final Long2ObjectHashMap<ReplaySession> replaySessions,
            final ControlSession controlSession,
            final long recordingId,
            final long stopPosition) {
        this.correlationId = correlationId;
        this.controlSession = controlSession;
        this.replayIterator = replaySessions.entrySet().iterator();
        this.recordingId = recordingId;
        this.stopPosition = stopPosition;
    }

    public int doWork()
    {
        int workCount = 0;

        if (!isDone)
        {
            try
            {
                System.out.printf("Starting stop slow replay check for recordingId: %d currentPosition: %d%n",
                        recordingId, stopPosition);

                while (replayIterator.hasNext())
                {
                    final ReplaySession session = replayIterator.next().getValue();
                    System.out.printf("Checking replay session %d recordingId: %d position: %d%n",
                            session.sessionId(), session.recordingId(), session.getReplayPosition());

                    if (session.recordingId() == recordingId)
                    {
                        final long replayPosition = session.getReplayPosition();
                        if (replayPosition < stopPosition)
                        {
                            System.out.printf("Stopping slow replay at position: %d%n", replayPosition);
                            controlSession.onStopReplay(correlationId, session.sessionId());
                            workCount++;
                        }
                    }
                }

                isDone = true;
                System.out.println("Found " + workCount + " replays to stop");
                controlSession.sendOkResponse(correlationId, workCount);
            }
            catch (final Exception ex)
            {
                isDone = true;
                System.out.println("Error in StopSlowReplaysSession: " + ex);
                controlSession.sendErrorResponse(correlationId, ex.getMessage());
            }
        }

        return workCount;
    }


    public void abort()
    {
        isDone = true;
    }

    public boolean isDone()
    {
        return isDone;
    }

    public long sessionId()
    {
        return correlationId;
    }

    public void close() {}
}