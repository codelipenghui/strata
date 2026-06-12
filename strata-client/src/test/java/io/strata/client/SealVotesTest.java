package io.strata.client;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SealVotesTest {

    @Test
    void prefersLargestAgreeingGroup() {
        SealVotes votes = new SealVotes();
        votes.add(1, 11, 1);
        votes.add(1, 11, 2);
        votes.add(1, 22, 1);
        votes.add(1, 22, 2);
        votes.add(1, 22, 3);

        Map.Entry<SealVotes.Key, List<Integer>> best = votes.best(2);

        assertEquals(new SealVotes.Key(1, 22), best.getKey());
        assertEquals(List.of(1, 2, 3), best.getValue());
        assertEquals(5, votes.total());
        assertTrue(votes.divergent());
    }

    @Test
    void keepsFirstQuorumWhenVotesTie() {
        SealVotes votes = new SealVotes();
        votes.add(1, 11, 1);
        votes.add(1, 11, 2);
        votes.add(1, 22, 2);
        votes.add(1, 22, 3);

        Map.Entry<SealVotes.Key, List<Integer>> best = votes.best(2);

        assertEquals(new SealVotes.Key(1, 11), best.getKey());
        assertEquals(List.of(1, 2), best.getValue());
    }

    @Test
    void skipsGroupsBelowQuorumAndReportsNoWinner() {
        SealVotes votes = new SealVotes();
        votes.add(4, 10, 9);
        votes.add(4, 11, 1);
        votes.add(4, 11, 2);

        assertEquals(new SealVotes.Key(4, 11), votes.best(2).getKey());
        assertNull(votes.best(3));
    }
}
