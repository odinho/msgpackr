import { Packr } from './pack.js'
import { Unpackr } from './unpack.js'

/**
 * Helper function to concatenate two Uint8Arrays
 */
function concatUint8Arrays(a, b) {
	const combined = new Uint8Array(a.length + b.length)
	combined.set(a)
	combined.set(b, a.length)
	return combined
}

/**
 * Creates a Web Streams API TransformStream for packing JavaScript values into MessagePack format.
 * This is the browser-compatible version that works with ReadableStream/WritableStream.
 *
 * @param {Object} options - Configuration options for the Packr
 * @param {Packr} [options.packr] - Optional existing Packr instance to use
 * @returns {TransformStream} A TransformStream that accepts JavaScript values and outputs MessagePack binary data
 */
export function PackrStream(options = {}) {
	// sequential mode is required for streaming to ensure proper message boundaries
	const packrOptions = { ...options, sequential: true }
	const packr = options.packr || new Packr(packrOptions)

	return new TransformStream({
		transform(value, controller) {
			try {
				const packed = packr.pack(value)
				controller.enqueue(packed)
			} catch (error) {
				controller.error(error)
			}
		}
	})
}

/**
 * Creates a Web Streams API TransformStream for unpacking MessagePack data into JavaScript values.
 * Handles chunked data, multiple values per chunk, backpressure, and error propagation.
 *
 * @param {Object} options - Configuration options for the Unpackr
 * @param {Unpackr} [options.unpackr] - Optional existing Unpackr instance to use
 * @returns {TransformStream} A TransformStream that accepts MessagePack binary data and outputs JavaScript values
 */
export function UnpackrStream(options = {}) {
	// Initialize structures array if not provided (required for record extension)
	const unpackrOptions = options.structures ? options : { ...options, structures: [] }
	const unpackr = options.unpackr || new Unpackr(unpackrOptions)

	let incompleteBuffer = null

	return new TransformStream({
		transform(chunk, controller) {
			try {
				if (!(chunk instanceof Uint8Array)) {
					if (ArrayBuffer.isView(chunk)) {
						chunk = new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
					} else if (chunk instanceof ArrayBuffer) {
						chunk = new Uint8Array(chunk)
					} else {
						throw new TypeError('Chunk must be a Uint8Array or ArrayBuffer')
					}
				}

				if (incompleteBuffer) {
					chunk = concatUint8Arrays(incompleteBuffer, chunk)
					incompleteBuffer = null
				}

				let values = null
				try {
					values = unpackr.unpackMultiple(chunk)
				} catch (error) {
					// Handle incomplete MessagePack data
					if (error.incomplete) {
						// Store the incomplete portion for next chunk
						incompleteBuffer = chunk.slice(error.lastPosition)
						// Use any successfully parsed values
						values = error.values
					} else {
						// Re-throw non-incomplete errors
						throw error
					}
				}

				if (values) {
					for (let value of values) {
						// Web Streams can handle null values directly (unlike Node.js streams)
						// controller.enqueue(null) is valid; controller.close() signals end-of-stream
						controller.enqueue(value)
					}
				}
			} catch (error) {
				controller.error(error)
			}
		},

		flush(controller) {
			// If there's still data in incomplete buffer when stream ends, it's an error
			if (incompleteBuffer && incompleteBuffer.length > 0) {
				controller.error(new Error('Stream ended with incomplete MessagePack data'))
			}
		}
	})
}
