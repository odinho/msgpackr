import { PackrStream, UnpackrStream } from '../web-stream.js'
import { Packr, Unpackr } from '../index.js'
import chai from 'chai'

const assert = chai.assert

// Helper to concatenate Uint8Arrays (web-compatible alternative to Buffer.concat)
function concatUint8Arrays(...arrays) {
	const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0)
	const result = new Uint8Array(totalLength)
	let offset = 0
	for (const arr of arrays) {
		result.set(arr, offset)
		offset += arr.length
	}
	return result
}

suite('msgpackr web stream tests', function() {
	test('basic unpack stream with single value', async () => {
		const packr = new Packr()
		const data = { name: 'test', value: 42 }
		const packed = packr.pack(data)

		const unpackStream = UnpackrStream()
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(packed)
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		const result = await reader.read()
		assert.equal(result.done, false)
		assert.deepEqual(result.value, data)

		const end = await reader.read()
		assert.equal(end.done, true)
	})

	test('unpack stream with multiple values', async () => {
		const packr = new Packr()
		const messages = [
			{ name: 'first' },
			{ name: 'second' },
			{ name: 'third', extra: [1, 3, { foo: 'hi' }, 'bye'] }
		]

		const packed = concatUint8Arrays(...messages.map(m => packr.pack(m)))

		const unpackStream = UnpackrStream()
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(packed)
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		const received = []
		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			received.push(value)
		}

		assert.deepEqual(received, messages)
	})

	test('unpack stream with chunked data', async () => {
		const packr = new Packr()
		const data = { name: 'test', value: [1, 2, 3, 4, 5], nested: { foo: 'bar' } }
		const packed = packr.pack(data)

		const chunk1 = packed.slice(0, 10)
		const chunk2 = packed.slice(10, 20)
		const chunk3 = packed.slice(20)

		const unpackStream = UnpackrStream()
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(chunk1)
				controller.enqueue(chunk2)
				controller.enqueue(chunk3)
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		const result = await reader.read()
		assert.equal(result.done, false)
		assert.deepEqual(result.value, data)

		const end = await reader.read()
		assert.equal(end.done, true)
	})

	test('unpack stream with multiple values across chunks', async () => {
		const packr = new Packr()
		const messages = [
			{ id: 1, name: 'first' },
			{ id: 2, name: 'second' },
			{ id: 3, name: 'third' }
		]

		const packed = concatUint8Arrays(...messages.map(m => packr.pack(m)))

		// Split in the middle of messages to test incomplete buffer handling
		const chunk1 = packed.slice(0, 15)
		const chunk2 = packed.slice(15)

		const unpackStream = UnpackrStream()
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(chunk1)
				controller.enqueue(chunk2)
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		const received = []
		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			received.push(value)
		}

		assert.deepEqual(received, messages)
	})

	test('unpack stream with ArrayBuffer input', async () => {
		const packr = new Packr()
		const data = { test: 'value' }
		const packed = packr.pack(data)
		const arrayBuffer = packed.buffer.slice(packed.byteOffset, packed.byteOffset + packed.byteLength)

		const unpackStream = UnpackrStream()
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(arrayBuffer)
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		const result = await reader.read()
		assert.deepEqual(result.value, data)
	})

	test('unpack stream handles null values correctly', async () => {
		const packr = new Packr()
		const messages = [
			{ value: 'first' },
			null,
			{ value: 'third' }
		]

		const packed = concatUint8Arrays(...messages.map(m => packr.pack(m)))

		const unpackStream = UnpackrStream()
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(packed)
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		const received = []
		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			received.push(value)
		}

		assert.equal(received.length, 3)
		assert.deepEqual(received[0], { value: 'first' })
		assert.strictEqual(received[1], null)
		assert.deepEqual(received[2], { value: 'third' })
	})

	test('unpack stream with records/structures', async () => {
		const packr = new Packr({ useRecords: true })
		const unpackr = new Unpackr({ useRecords: true })

		const messages = [
			{ name: 'Alice', age: 30 },
			{ name: 'Bob', age: 25 },
			{ name: 'Charlie', age: 35 }
		]

		const packed = concatUint8Arrays(...messages.map(m => packr.pack(m)))

		const unpackStream = UnpackrStream({ unpackr })
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(packed)
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		const received = []
		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			received.push(value)
		}

		assert.deepEqual(received, messages)
	})

	test('unpack stream error handling - incomplete data at end', async () => {
		const packr = new Packr()
		const data = { name: 'test', value: 42 }
		const packed = packr.pack(data)

		const incomplete = packed.slice(0, packed.length - 3)

		const unpackStream = UnpackrStream()

		try {
			const reader = new ReadableStream({
				start(controller) {
					controller.enqueue(incomplete)
					controller.close()
				}
			})
				.pipeThrough(unpackStream)
				.getReader()

			while (true) {
				const { done } = await reader.read()
				if (done) break
			}

			assert.fail('Should have thrown an error for incomplete data')
		} catch (error) {
			assert.include(error.message.toLowerCase(), 'incomplete')
		}
	})

	test('unpack stream error handling - invalid MessagePack data', async () => {
		const invalidData = new Uint8Array([0xc1]) // Reserved/invalid MessagePack token

		const unpackStream = UnpackrStream()

		try {
			const reader = new ReadableStream({
				start(controller) {
					controller.enqueue(invalidData)
					controller.close()
				}
			})
				.pipeThrough(unpackStream)
				.getReader()

			await reader.read()
			assert.fail('Should have thrown an error for invalid data')
		} catch (error) {
			assert.isTrue(error instanceof Error)
		}
	})

	test('pack stream with single value', async () => {
		const data = { name: 'test', value: 42 }

		const packStream = PackrStream()
		const chunks = []

		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(data)
				controller.close()
			}
		})
			.pipeThrough(packStream)
			.getReader()

		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			chunks.push(value)
		}

		assert.equal(chunks.length, 1)
		const unpackr = new Unpackr()
		const unpacked = unpackr.unpack(chunks[0])
		assert.deepEqual(unpacked, data)
	})

	test('pack stream with multiple values', async () => {
		const messages = [
			{ name: 'first' },
			{ name: 'second' },
			{ name: 'third' }
		]

		const packStream = PackrStream()
		const received = []

		// Use both pack and unpack streams for a round-trip test
		const reader = new ReadableStream({
			start(controller) {
				for (const msg of messages) {
					controller.enqueue(msg)
				}
				controller.close()
			}
		})
			.pipeThrough(packStream)
			.pipeThrough(UnpackrStream())
			.getReader()

		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			received.push(value)
		}

		assert.deepEqual(received, messages)
	})

	test('round-trip: pack and unpack stream', async () => {
		const messages = [
			{ id: 1, name: 'Alice' },
			{ id: 2, name: 'Bob' },
			{ id: 3, name: 'Charlie' }
		]

		const packStream = PackrStream()
		const unpackStream = UnpackrStream()

		const received = []
		const reader = new ReadableStream({
			start(controller) {
				for (const msg of messages) {
					controller.enqueue(msg)
				}
				controller.close()
			}
		})
			.pipeThrough(packStream)
			.pipeThrough(unpackStream)
			.getReader()

		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			received.push(value)
		}

		assert.deepEqual(received, messages)
	})

	test('stream with custom Packr options', async () => {
		const packr = new Packr({ useRecords: true })
		const data = { name: 'test', value: 42 }

		const packStream = PackrStream({ packr })
		const reader = new ReadableStream({
			start(controller) {
				controller.enqueue(data)
				controller.close()
			}
		})
			.pipeThrough(packStream)
			.getReader()

		const result = await reader.read()
		assert.equal(result.done, false)
		assert.isTrue(result.value instanceof Uint8Array)
	})

	test('stream with many values in chunks', async () => {
		// Create many messages to test chunked streaming with large datasets
		const messages = Array.from({ length: 100 }, (_, i) => ({
			id: i,
			data: `message ${i}`,
			array: [1, 2, 3, 4, 5]
		}))

		const packr = new Packr()
		const packed = concatUint8Arrays(...messages.map(m => packr.pack(m)))

		const unpackStream = UnpackrStream()
		const received = []

		const reader = new ReadableStream({
			start(controller) {
				let offset = 0
				const chunkSize = 100
				while (offset < packed.length) {
					controller.enqueue(packed.slice(offset, offset + chunkSize))
					offset += chunkSize
				}
				controller.close()
			}
		})
			.pipeThrough(unpackStream)
			.getReader()

		while (true) {
			const { done, value } = await reader.read()
			if (done) break
			received.push(value)
		}

		assert.equal(received.length, messages.length)
		assert.deepEqual(received, messages)
	})
})
