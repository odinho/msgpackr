export { FLOAT32_OPTIONS, Options } from './index.js'

/**
 * Creates a TransformStream for unpacking MessagePack data in Web Streams API environments.
 *
 * @param options - Optional Unpackr options for configuring the unpacking behavior
 * @returns A TransformStream that accepts Uint8Array chunks and outputs unpacked values
 *
 * @example
 * ```typescript
 * import { UnpackrStream } from 'msgpackr/web-stream'
 *
 * const response = await fetch('/data.msgpack')
 * const unpackStream = UnpackrStream()
 *
 * for await (const value of response.body.pipeThrough(unpackStream)) {
 *   console.log(value)
 * }
 * ```
 */
export function UnpackrStream<T = any>(options?: import('./index.js').Options): TransformStream<Uint8Array | ArrayBuffer, T>

/**
 * Creates a TransformStream for packing values into MessagePack format in Web Streams API environments.
 *
 * @param options - Optional Packr options for configuring the packing behavior
 * @returns A TransformStream that accepts values and outputs Uint8Array chunks
 *
 * @example
 * ```typescript
 * import { PackrStream } from 'msgpackr/web-stream'
 *
 * const packStream = PackrStream()
 * const writableStream = getWritableStream() // e.g., from fetch or file
 *
 * const writer = packStream.readable.pipeTo(writableStream)
 * const streamWriter = packStream.writable.getWriter()
 *
 * await streamWriter.write({ foo: 'bar' })
 * await streamWriter.write([1, 2, 3])
 * await streamWriter.close()
 * await writer
 * ```
 */
export function PackrStream<T = any>(options?: import('./index.js').Options): TransformStream<T, Uint8Array>
