/**
 * Contract: a directed reply the host computes across an await is delivered to the guest
 * that asked, or to nobody.
 *
 * The relay mints peer ids per room from 1 and frees the room the moment the host socket
 * closes, so a host that drops and reclaims meets a *different* guest holding the number
 * its unfinished handler captured. `CollabSocket.send` therefore takes the `legId` that
 * came with the request, and every host handler that answers after an await has to pass
 * it — the leg capture inside `send` only sees the leg the reply is finally sent on, which
 * by then is live, addressable and wrong.
 *
 * The socket-level suites pin the gate; these cases pin the callsites, because a handler
 * that forgets the argument is invisible to both the gate and the type checker.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { importRoomKey } from "../../src/collab/crypto";
import { CollabHost } from "../../src/collab/host";
import { COLLAB_PROTO, type CollabFrame, parseCollabLink } from "../../src/collab/protocol";
import { CollabSocket } from "../../src/collab/relay-client";
import type { InteractiveModeContext } from "../../src/modes/types";
import { AgentRegistry } from "../../src/registry/agent-registry";
import type { AgentSession } from "../../src/session/agent-session";
import {
	hostLeg,
	installRecyclingRelay,
	type RecyclingRelay,
	uninstallRecyclingRelay,
} from "./helpers/recycling-relay";

/** Mirrors LEG_SETTLE_MS in the client: a reclaimed leg is provisional for this long. */
const LEG_SETTLE_MS = 2_000;
/** How long a reply is given to reach the wire after its handler unparks. */
const REPLY_WINDOW_MS = 400;

interface HostHarness {
	ctx: InteractiveModeContext;
	/** Prompt turns the host is waiting on, newest last; rejecting one answers its guest. */
	promptCalls: { reject: (err: Error) => void }[];
}

function makeHostContext(): HostHarness {
	const promptCalls: { reject: (err: Error) => void }[] = [];
	const ctx = {
		settings: { get: () => "" },
		sessionManager: {
			getSessionId: () => "sess-reply-leg",
			getCwd: () => "/tmp",
			getSessionFile: () => undefined,
			snapshotForReplication: () => ({
				header: { type: "session", id: "sess-reply-leg", timestamp: new Date().toISOString(), cwd: "/tmp" },
				entries: [],
			}),
			onEntryAppended: undefined,
		},
		session: {
			isStreaming: false,
			queuedMessageCount: 0,
			sessionName: "reply-leg",
			model: undefined,
			thinkingLevel: undefined,
			subscribe: () => () => {},
			emitNotice: () => {},
			promptCustomMessage: () => {
				const { promise, reject } = Promise.withResolvers<void>();
				promptCalls.push({ reject });
				return promise;
			},
			abort: () => Promise.resolve(),
		},
		eventBus: undefined,
		statusLine: {
			setCollabStatus: () => {},
			invalidate: () => {},
			getCachedContextBreakdown: () => ({ usedTokens: 0, contextWindow: 0 }),
		},
		ui: { requestRender: () => {} },
		showStatus: () => {},
		showError: () => {},
		updatePendingMessagesDisplay: () => {},
		collabHost: undefined,
	} as unknown as InteractiveModeContext;
	return { ctx, promptCalls };
}

interface TestGuest {
	socket: CollabSocket;
	frames: CollabFrame[];
	peerId: number;
}

const sleep = (ms: number): Promise<void> => {
	const { promise, resolve } = Promise.withResolvers<void>();
	setTimeout(resolve, ms);
	return promise;
};

let harness: HostHarness;
let host: CollabHost;
let relay: RecyclingRelay;
let tmpDir: string;
const openGuests: CollabSocket[] = [];

async function joinAsGuest(name: string): Promise<TestGuest> {
	const parsed = parseCollabLink(host.link);
	if ("error" in parsed) throw new Error(parsed.error);
	const writeToken = parsed.writeToken ? Buffer.from(parsed.writeToken).toString("base64url") : undefined;
	const key = await importRoomKey(parsed.key);
	const socket = new CollabSocket({ wsUrl: parsed.wsUrl, role: "guest", key });
	openGuests.push(socket);
	const frames: CollabFrame[] = [];
	const welcomed = Promise.withResolvers<void>();
	socket.onFrame = frame => {
		frames.push(frame);
		if (frame.t === "welcome") welcomed.resolve();
	};
	socket.onOpen = () => socket.send({ t: "hello", proto: COLLAB_PROTO, name, writeToken });
	socket.connect();
	await welcomed.promise;
	return { socket, frames, peerId: relay.minted.at(-1) ?? 0 };
}

/** Drops the host's transport half-open, then waits for the reclaimed leg to open and settle. */
async function reclaimRoom(): Promise<void> {
	const roomsBefore = relay.rooms;
	hostLeg().dropTransient();
	const deadline = Date.now() + 15_000;
	while (relay.rooms === roomsBefore && Date.now() < deadline) await sleep(25);
	if (relay.rooms === roomsBefore) throw new Error("the host never reconnected");
	await sleep(LEG_SETTLE_MS + 400);
}

beforeEach(async () => {
	tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "collab-reply-leg-"));
	relay = installRecyclingRelay();
	harness = makeHostContext();
	host = new CollabHost(harness.ctx);
	await host.start("ws://localhost:8787");
});

afterEach(async () => {
	for (const socket of openGuests.splice(0)) socket.close();
	uninstallRecyclingRelay();
	await host.stop("test done");
	await fs.rm(tmpDir, { recursive: true, force: true });
});

describe("CollabHost directed replies across a reclaim", () => {
	it("does not hand a failed prompt's error to the guest that inherited the peer id", async () => {
		const guestA = await joinAsGuest("guest-A");
		expect(guestA.peerId).toBe(1);
		// A steered prompt settles when the turn that absorbed it does — seconds to minutes,
		// far longer than a reclaim takes.
		guestA.socket.send({ t: "prompt", text: "the prompt guest A typed" });
		const deadline = Date.now() + 5_000;
		while (harness.promptCalls.length === 0 && Date.now() < deadline) await sleep(25);
		expect(harness.promptCalls).toHaveLength(1);

		guestA.socket.close();
		await reclaimRoom();
		const guestB = await joinAsGuest("guest-B");
		// The crux: the fresh room handed guest B the number guest A's prompt captured.
		expect(guestB.peerId).toBe(guestA.peerId);

		harness.promptCalls[0]?.reject(new Error("guest A's prompt failed"));
		await sleep(REPLY_WINDOW_MS);

		expect(guestB.frames.filter(frame => frame.t === "error")).toEqual([]);
	}, 30_000);

	it("does not hand a failed agent command's error to the guest that inherited the peer id", async () => {
		const registry = AgentRegistry.global();
		const abortGate = Promise.withResolvers<void>();
		const ref = registry.register({
			id: "Reply-Leg-Agent",
			displayName: "Reply-Leg-Agent",
			kind: "sub",
			session: { abort: () => abortGate.promise, dispose: async () => {} } as unknown as AgentSession,
			sessionFile: path.join(tmpDir, "agent.jsonl"),
			status: "running",
		});
		try {
			const guestA = await joinAsGuest("guest-A");
			expect(guestA.peerId).toBe(1);
			// `kill` awaits the agent's abort, which the gate above holds open across the drop.
			guestA.socket.send({ t: "agent-cmd", cmd: "kill", agentId: "Reply-Leg-Agent" });
			await sleep(200);

			guestA.socket.close();
			await reclaimRoom();
			const guestB = await joinAsGuest("guest-B");
			expect(guestB.peerId).toBe(guestA.peerId);

			abortGate.reject(new Error("guest A's kill failed"));
			await sleep(REPLY_WINDOW_MS);

			expect(guestB.frames.filter(frame => frame.t === "error")).toEqual([]);
		} finally {
			registry.unregister("Reply-Leg-Agent", ref);
		}
	}, 30_000);

	it("still answers the guest that asked when no leg was lost", async () => {
		const guest = await joinAsGuest("guest-A");
		guest.socket.send({ t: "prompt", text: "a prompt that fails on its own leg" });
		const deadline = Date.now() + 5_000;
		while (harness.promptCalls.length === 0 && Date.now() < deadline) await sleep(25);

		harness.promptCalls[0]?.reject(new Error("this turn failed"));
		await sleep(REPLY_WINDOW_MS);

		expect(guest.frames.filter(frame => frame.t === "error")).toHaveLength(1);
	}, 30_000);
});
