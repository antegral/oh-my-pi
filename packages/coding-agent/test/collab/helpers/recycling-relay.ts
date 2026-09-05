/**
 * In-memory relay that recycles peer ids the way the reference relay does.
 *
 * `packages/collab-web/scripts/local-relay.ts` keys rooms by roomId, mints guest ids from
 * `nextPeerId: 1`, and `rooms.delete(roomId)` the moment the host socket closes. So a host
 * that drops and reconnects gets a brand new room whose first guest is peer 1 again — the
 * precondition for a reply that outlived its own leg being handed to the wrong guest.
 * `helpers/in-memory-relay.ts` never frees the room, so its ids only ever climb and that
 * collision cannot happen there; this fixture frees it.
 */
import { packEnvelope, rewriteEnvelopePeer, unpackEnvelope } from "../../../src/collab/protocol";

let activeRelay: RecyclingRelay | null = null;
const RealWebSocket = globalThis.WebSocket;

export class RelayWebSocket {
	static readonly CONNECTING = 0;
	static readonly OPEN = 1;
	static readonly CLOSING = 2;
	static readonly CLOSED = 3;
	static instances: RelayWebSocket[] = [];

	binaryType = "blob";
	bufferedAmount = 0;
	readyState: number = RelayWebSocket.CONNECTING;
	readonly role: "host" | "guest";
	peerId = 0;
	onopen: (() => void) | null = null;
	onmessage: ((event: { data: unknown }) => void) | null = null;
	onerror: (() => void) | null = null;
	onclose: ((event: { code: number; reason: string }) => void) | null = null;
	readonly #relay: RecyclingRelay;

	constructor(url: string) {
		const relay = activeRelay;
		if (!relay) throw new Error("RelayWebSocket: no active relay");
		this.#relay = relay;
		this.role = new URL(url).searchParams.get("role") === "host" ? "host" : "guest";
		RelayWebSocket.instances.push(this);
		queueMicrotask(() => {
			if (this.readyState !== RelayWebSocket.CONNECTING) return;
			this.readyState = RelayWebSocket.OPEN;
			relay.connect(this);
			this.onopen?.();
		});
	}

	send(data: Uint8Array): void {
		if (this.readyState !== RelayWebSocket.OPEN) return;
		const bytes = new Uint8Array(data);
		queueMicrotask(() => this.#relay.forward(this, bytes));
	}

	close(_code?: number): void {
		if (this.readyState === RelayWebSocket.CLOSED) return;
		this.readyState = RelayWebSocket.CLOSED;
		this.#relay.disconnect(this);
		queueMicrotask(() => this.onclose?.({ code: 1000, reason: "closed" }));
	}

	/** Half-open drop: the transport dies under the client with a transient code. */
	dropTransient(): void {
		if (this.readyState === RelayWebSocket.CLOSED) return;
		this.readyState = RelayWebSocket.CLOSED;
		this.#relay.disconnect(this);
		this.onclose?.({ code: 1006, reason: "Connection ended" });
	}

	deliver(bytes: Uint8Array): void {
		if (this.readyState !== RelayWebSocket.OPEN) return;
		const copy = new Uint8Array(bytes);
		queueMicrotask(() => this.onmessage?.({ data: copy.buffer }));
	}

	deliverControl(json: string): void {
		if (this.readyState !== RelayWebSocket.OPEN) return;
		queueMicrotask(() => this.onmessage?.({ data: json }));
	}
}

export class RecyclingRelay {
	#host: RelayWebSocket | null = null;
	#guests = new Map<number, RelayWebSocket>();
	#nextPeerId = 1;
	/** Peer ids minted per host registration, in order, for the assertions below. */
	readonly minted: number[] = [];
	rooms = 0;
	/** Fires as the relay forwards a guest frame to the host, before the host observes it. */
	onGuestFrame: ((peerId: number) => void) | null = null;

	get hostConnected(): boolean {
		return this.#host !== null;
	}

	connect(ws: RelayWebSocket): void {
		if (ws.role === "host") {
			if (this.#host) return; // a second host would get 4009; not modelled here
			this.#host = ws;
			this.#nextPeerId = 1;
			this.#guests = new Map();
			this.rooms++;
			return;
		}
		if (!this.#host) return;
		ws.peerId = this.#nextPeerId++;
		this.minted.push(ws.peerId);
		this.#guests.set(ws.peerId, ws);
		this.#host.deliverControl(JSON.stringify({ t: "peer-joined", peer: ws.peerId }));
	}

	forward(from: RelayWebSocket, bytes: Uint8Array): void {
		if (from.role === "host") {
			if (this.#host !== from) return;
			const envelope = unpackEnvelope(bytes);
			if (!envelope) return;
			if (envelope.peerId === 0) {
				for (const guest of this.#guests.values()) guest.deliver(bytes);
			} else {
				this.#guests.get(envelope.peerId)?.deliver(bytes);
			}
			return;
		}
		if (!this.#guests.has(from.peerId) || this.#guests.get(from.peerId) !== from) return;
		rewriteEnvelopePeer(bytes, from.peerId);
		this.onGuestFrame?.(from.peerId);
		this.#host?.deliver(bytes);
	}

	/** Room teardown mirrors the reference relay: host close frees the room and kicks guests. */
	disconnect(ws: RelayWebSocket): void {
		if (ws.role === "host") {
			if (this.#host !== ws) return;
			this.#host = null;
			const closure = JSON.stringify({ t: "room-closed" });
			for (const guest of this.#guests.values()) {
				guest.deliverControl(closure);
				queueMicrotask(() => guest.onclose?.({ code: 4001, reason: "room closed" }));
				guest.readyState = RelayWebSocket.CLOSED;
			}
			this.#guests = new Map();
			return;
		}
		if (this.#guests.get(ws.peerId) !== ws) return;
		this.#guests.delete(ws.peerId);
		this.#host?.deliverControl(JSON.stringify({ t: "peer-left", peer: ws.peerId }));
	}
}

export function installRecyclingRelay(): RecyclingRelay {
	activeRelay = new RecyclingRelay();
	RelayWebSocket.instances = [];
	globalThis.WebSocket = RelayWebSocket as unknown as typeof WebSocket;
	return activeRelay;
}

export function uninstallRecyclingRelay(): void {
	globalThis.WebSocket = RealWebSocket;
	activeRelay = null;
}

/** The host's live transport, for the half-open drop these cases need. */
export function hostLeg(): RelayWebSocket {
	const leg = RelayWebSocket.instances.filter(ws => ws.role === "host").at(-1);
	if (!leg) throw new Error("no host leg");
	return leg;
}

export { packEnvelope };
