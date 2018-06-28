const cbor = require('borc')
const multihashes = require('multihashes')
const crypto = require('crypto')
const CID = require('cids')
const Block = require('ipfs-block')
const isCircular = require('is-circular')

const sha2 = b => crypto.createHash('sha256').update(b).digest()

const CID_CBOR_TAG = 42

/* start copy from exisisting dag-cbor */
function tagCID (cid) {
  if (typeof cid === 'string') {
    cid = new CID(cid).buffer
  }

  return new cbor.Tagged(CID_CBOR_TAG, Buffer.concat([
    Buffer.from('00', 'hex'), // thanks jdag
    cid
  ]))
}

function replaceCIDbyTAG (dagNode) {
  let circular
  try {
    circular = isCircular(dagNode)
  } catch (e) {
    circular = false
  }
  if (circular) {
    throw new Error('The object passed has circular references')
  }

  function transform (obj) {
    if (!obj || Buffer.isBuffer(obj) || typeof obj === 'string') {
      return obj
    }

    if (Array.isArray(obj)) {
      return obj.map(transform)
    }

    const keys = Object.keys(obj)

    // only `{'/': 'link'}` are valid
    if (keys.length === 1 && keys[0] === '/') {
      // Multiaddr encoding
      // if (typeof link === 'string' && isMultiaddr(link)) {
      //  link = new Multiaddr(link).buffer
      // }

      return tagCID(obj['/'])
    } else if (keys.length > 0) {
      // Recursive transform
      let out = {}
      keys.forEach((key) => {
        if (typeof obj[key] === 'object') {
          out[key] = transform(obj[key])
        } else {
          out[key] = obj[key]
        }
      })
      return out
    } else {
      return obj
    }
  }

  return transform(dagNode)
}
/* end copy from existing dag-cbor */

const chunk = function * (obj, size = 500) {
  let i = 0
  let keys = Object.keys(obj)

  let _slice = (start, end) => {
    let o = {}
    keys.slice(start, end).forEach(k => {
      o[k] = obj[k]
    })
    return o
  }

  while (i < keys.length) {
    yield _slice(i, i + size)
    i = i + size
  }
  yield _slice(i)
}
const asBlock = (buffer, type) => {
  let hash = multihashes.encode(sha2(buffer), 'sha2-256')
  let cid = new CID(1, type, hash)
  return new Block(buffer, cid)
}

class NotFound extends Error {
  get code () {
    return 404
  }
}

class IPLD {
  constructor (get, maxsize = 1e+7 /* 10megs */) {
    this._get = get
    this._maxBlockSize = 1e+6 // 1meg
    this._maxSize = maxsize
    this._decoder = new cbor.Decoder({
      tags: {
        [CID_CBOR_TAG]: (val) => {
          val = val.slice(1)
          return {'/': val}
        }
      },
      size: maxsize
    })
  }
  get multicodec () {
    return 'dag-cbor'
  }
  _cid (buffer) {
    let hash = multihashes.encode(sha2(buffer), 'sha2-256')
    let cid = new CID(1, 'dag-cbor', hash)
    return cid.toBaseEncodedString()
  }
  async cids (buffer) {
    let self = this
    return (function * () {
      yield self._cid(buffer)
      let root = self._deserialize(buffer)
      if (root['._'] === 'dag-split') {
        let cids = root.chunks.map(b => b['/'])
        for (let cid of cids) {
          yield cid
        }
      }
    })()
    // return [iterable of cids]
  }
  async resolve (buffer, path) {
    if (!Array.isArray(path)) {
      path = path.split('/').filter(x => x)
    }
    let root = await this.deserialize(buffer)

    while (path.length) {
      let prop = path.shift()
      root = root[prop]
      if (typeof root === 'undefined') {
        throw NotFound(`Cannot find link "${prop}".`)
      }
      if (typeof root === 'object' && root['/']) {
        let c = new CID(root['/'])
        if (c.codec !== 'dag-cbor') {
          return {value: c, remaining: path.join('/')}
        }
        let buff = await this._get(c.toBaseEncodedString())
        return this.resolve(buff, path)
      }
    }
    return {value: root, remaining: path.join('/')}
  }
  _deserialize (buffer) {
    return this._decoder.decodeFirst(buffer)
  }
  _serialize (dagNode) {
    let dagNodeTagged = replaceCIDbyTAG(dagNode)
    return cbor.encode(dagNodeTagged)
  }
  serialize (dagNode) {
    // TODO: handle large objects
    let buffer = this._serialize(dagNode)
    if (buffer.length > this._maxSize) {
      throw new Error('cbor node is too large.')
    }
    let maxBlockSize = this._maxBlockSize
    let _serialize = this._serialize
    if (buffer.length > maxBlockSize) {
      return (function * () {
        let node = {'._': 'dag-split'}
        node.chunks = []
        for (let _chunk of chunk(dagNode)) {
          let block = asBlock(_serialize(_chunk), 'dag-cbor')
          yield block
          node.chunks.push({'/': block.cid.toBaseEncodedString()})
        }
        yield asBlock(_serialize(node), 'dag-cbor')
      })()
    } else {
      return [asBlock(buffer, 'dag-cbor')]
    }
    // return iterable of Blocks
  }
  async deserialize (buffer) {
    let root = this._deserialize(buffer)
    if (root['._'] === 'dag-split') {
      let cids = root.chunks.map(b => {
        return (new CID(b['/'])).toBaseEncodedString()
      })
      let blocks = cids.map(c => this._get(c))
      blocks = await Promise.all(blocks)
      return Object.assign(...blocks.map(b => this._deserialize(b)))
    } else {
      return root
    }
    // return native type
  }
  async tree (buffer) {
    // TODO: replaces streaming parsing for cbor using iterator.
    return Object.keys(await this.deserialize(buffer))
    // returns iterable of keys
  }
}

module.exports = (get) => new IPLD(get)
