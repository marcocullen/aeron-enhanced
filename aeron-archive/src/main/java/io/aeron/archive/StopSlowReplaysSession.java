package io.aeron.archive;

import org.agrona.collections.Long2ObjectHashMap;
import java.util.Iterator;

class StopSlowReplaysSession implements Session {
    private final long correlationId;
    private final Long2ObjectHashMap<ReplaySession> replaySessions;
    private final ControlSession controlSession;
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
        this.replaySessions = replaySessions;
        this.controlSession = controlSession;
        this.recordingId = recordingId;
        this.stopPosition = stopPosition;
    }

    @Override
    public int doWork() {
        int workCount = 0;

        if (!isDone) {
            try {
                System.out.printf("Starting stop slow replay check for recordingId: %d currentPosition: %d%n",
                        recordingId, stopPosition);

                final Iterator<ReplaySession> iterator = replaySessions.values().iterator();
                int stoppedReplays = 0;

                while (iterator.hasNext()) {
                    final ReplaySession session = iterator.next();
                    System.out.printf("Checking replay session %d recordingId: %d position: %d against stopPosition %d%n",
                            session.sessionId(), session.recordingId(), session.replayPosition(), stopPosition);

                    if (session.recordingId() == recordingId) {
                        final long replayPosition = session.replayPosition();
                        if (replayPosition < stopPosition) {
                            System.out.printf("Stopping slow replay at position: %d%n", replayPosition);
                            session.abort(); // Stop the replay session
                            stoppedReplays++;
                        }
                    }
                }

                isDone = true;
                System.out.printf("Stopped %d replays%n", stoppedReplays);
                controlSession.sendOkResponse(correlationId, stoppedReplays);
            } catch (final Exception ex) {
                isDone = true;
                System.out.println("Error in StopSlowReplaysSession: " + ex);
                controlSession.sendErrorResponse(correlationId, ex.getMessage());
            }
        }

        return workCount;
    }

    @Override
    public long sessionId() {
        return controlSession.sessionId();
    }

    @Override
    public void abort() {
        isDone = true;
    }

    @Override
    public boolean isDone() {
        return isDone;
    }

    @Override
    public void close() {
        //aborting the session will close
    }
}
