//! PreVote and CheckQuorum.

mod common;

use common::{drained, e, only_reply, peers_of};
use kvraft::raft::{Config, Message, Node, Role};
use kvraft::sim::Sim;
use kvraft::storage::MemStorage;

fn elected(sim: &mut Sim) -> (u64, u64) {
    assert!(sim.run_until(300, |s| s.leader().is_some()));
    sim.run(30);
    let l = sim.leader().unwrap();
    (l, sim.nodes[&l].term())
}

// ---------------------------------------------------------------------------
// Whole-cluster behaviour
// ---------------------------------------------------------------------------

#[test]
fn isolated_follower_does_not_inflate_its_term_or_disturb_the_cluster() {
    let mut sim = Sim::new(3, 21);
    let (leader, cluster_term) = elected(&mut sim);
    let victim = (1..=3).find(|&id| id != leader).unwrap();

    sim.isolate(victim);
    sim.run(300);
    assert_eq!(
        sim.nodes[&victim].term(),
        cluster_term,
        "with PreVote an isolated node can never win a pre-vote, so it must not bump its term"
    );
    assert_ne!(sim.nodes[&victim].role(), Role::Leader);

    sim.heal(victim);
    sim.run(100);
    assert_eq!(
        sim.leader(),
        Some(leader),
        "rejoining must not dethrone the leader"
    );
    for n in sim.nodes.values() {
        assert_eq!(n.term(), cluster_term, "no election should have happened");
    }
}

/// The same scenario with PreVote off, to show what the feature buys.
#[test]
fn without_prevote_an_isolated_node_inflates_its_term_and_disrupts_on_rejoin() {
    let cfg = Config {
        seed: 21,
        pre_vote: false,
        ..Config::default()
    };
    let mut sim = Sim::with_config(3, cfg);
    let (leader, cluster_term) = elected(&mut sim);
    let victim = (1..=3).find(|&id| id != leader).unwrap();

    sim.isolate(victim);
    sim.run(300);
    assert!(
        sim.nodes[&victim].term() > cluster_term + 5,
        "without PreVote the lonely node keeps starting elections and raising its term"
    );

    sim.heal(victim);
    sim.run(100);
    let max_term = sim.nodes.values().map(|n| n.term()).max().unwrap();
    assert!(
        max_term > cluster_term + 5,
        "its inflated term should have forced the healthy cluster into a new election"
    );
}

#[test]
fn check_quorum_makes_a_cut_off_leader_step_down() {
    let mut sim = Sim::new(3, 5);
    let (leader, _) = elected(&mut sim);
    sim.isolate(leader);
    // Within two election windows it must notice it hears from nobody.
    sim.run(2 * Config::default().election_ticks + 5);
    assert_ne!(
        sim.nodes[&leader].role(),
        Role::Leader,
        "a leader that lost its majority must step down"
    );
}

#[test]
fn without_check_quorum_a_cut_off_leader_lingers() {
    let cfg = Config {
        seed: 5,
        check_quorum: false,
        ..Config::default()
    };
    let mut sim = Sim::with_config(3, cfg);
    let (leader, _) = elected(&mut sim);
    sim.isolate(leader);
    sim.run(200);
    assert_eq!(sim.nodes[&leader].role(), Role::Leader);
}

#[test]
fn healthy_leader_survives_check_quorum() {
    // CheckQuorum must not cause spurious step-downs on a healthy network.
    for seed in 1..=20 {
        let mut sim = Sim::new(5, seed);
        let (leader, term) = elected(&mut sim);
        sim.run(500);
        assert_eq!(sim.leader(), Some(leader), "seed {seed}");
        assert_eq!(sim.nodes[&leader].term(), term, "seed {seed}");
    }
}

// ---------------------------------------------------------------------------
// Single-node protocol tests
// ---------------------------------------------------------------------------

fn follower_with_live_leader() -> Node<MemStorage> {
    let mut n = Node::new(1, peers_of(1, 3), Config::default(), MemStorage::new());
    n.step(
        2,
        Message::AppendEntries {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e(2, b"x")],
            leader_commit: 0,
        },
    );
    drained(&mut n);
    assert_eq!(n.leader_id(), Some(2));
    n
}

fn pre_vote(term: u64, lli: u64, llt: u64) -> Message {
    Message::PreVote {
        term,
        last_log_index: lli,
        last_log_term: llt,
    }
}

#[test]
fn prevote_is_refused_while_a_leader_is_alive() {
    let mut n = follower_with_live_leader();
    n.step(3, pre_vote(3, 1, 2));
    assert_eq!(
        only_reply(&mut n),
        Message::PreVoteReply {
            term: 2,
            granted: false
        }
    );
    assert_eq!(n.term(), 2);
    assert_eq!(n.voted_for(), None);
}

#[test]
fn prevote_is_granted_once_the_leader_has_gone_quiet_and_changes_no_state() {
    let mut n = follower_with_live_leader();
    for _ in 0..Config::default().election_ticks {
        n.tick();
    }
    drained(&mut n); // discard anything the node sent on its own

    n.step(3, pre_vote(3, 1, 2));
    assert_eq!(
        only_reply(&mut n),
        Message::PreVoteReply {
            term: 3,
            granted: true
        }
    );
    // A pre-vote is only a question: no term bump, no vote cast.
    assert_eq!(n.term(), 2);
    assert_eq!(n.voted_for(), None);
}

#[test]
fn granting_a_prevote_does_not_use_up_the_real_vote() {
    let mut n = follower_with_live_leader();
    for _ in 0..Config::default().election_ticks {
        n.tick();
    }
    drained(&mut n);

    n.step(3, pre_vote(3, 1, 2));
    drained(&mut n);

    // A real vote request for the node's CURRENT term (2). It has to be the
    // same term: a request for a higher term would reset `voted_for` anyway and
    // so could not tell whether the pre-vote had wrongly recorded a vote.
    n.step(
        2,
        Message::RequestVote {
            term: 2,
            last_log_index: 1,
            last_log_term: 2,
        },
    );
    assert_eq!(
        only_reply(&mut n),
        Message::RequestVoteReply {
            term: 2,
            granted: true
        }
    );
    assert_eq!(n.voted_for(), Some(2));
}

#[test]
fn prevote_from_a_candidate_with_a_stale_log_is_refused_even_without_a_leader() {
    let mut n = follower_with_live_leader();
    for _ in 0..Config::default().election_ticks {
        n.tick();
    }
    drained(&mut n);

    // Our log ends at (term 2, index 1); this candidate has nothing.
    n.step(3, pre_vote(3, 0, 0));
    assert_eq!(
        only_reply(&mut n),
        Message::PreVoteReply {
            term: 2,
            granted: false
        }
    );
}

/// Drive a fresh node until it starts polling, returning it.
fn precandidate() -> Node<MemStorage> {
    let mut n = Node::new(1, peers_of(1, 3), Config::default(), MemStorage::new());
    for _ in 0..40 {
        n.tick();
        if n.role() == Role::PreCandidate {
            break;
        }
    }
    assert_eq!(n.role(), Role::PreCandidate);
    assert_eq!(n.term(), 0, "polling must not spend a term");
    let sent = drained(&mut n);
    assert!(
        sent.iter()
            .all(|m| matches!(m, Message::PreVote { term: 1, .. }))
    );
    n
}

#[test]
fn a_pre_vote_majority_turns_into_a_real_election() {
    let mut n = precandidate();
    n.step(
        2,
        Message::PreVoteReply {
            term: 1,
            granted: true,
        },
    );
    assert_eq!(n.role(), Role::Candidate);
    assert_eq!(n.term(), 1);
    assert_eq!(n.voted_for(), Some(1));
}

#[test]
fn a_grant_for_the_wrong_term_is_ignored() {
    let mut n = precandidate();
    n.step(
        2,
        Message::PreVoteReply {
            term: 5,
            granted: true,
        },
    );
    assert_eq!(n.role(), Role::PreCandidate);
    assert_eq!(n.term(), 0);
}

#[test]
fn a_rejection_from_a_higher_term_makes_the_precandidate_catch_up() {
    let mut n = precandidate();
    n.step(
        2,
        Message::PreVoteReply {
            term: 9,
            granted: false,
        },
    );
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(n.term(), 9);
}
