const fancyCBOR = require('./fancy-cbor')
const CID = require('cids')

const nest = (map, key, cid) => {
  while (key.length > 1) {
    let _key = key.shift()
    if (!map.has(_key)) map.set(_key, new Map())
    map = map.get(_key)
  }
  map.set(key.shift(), cid)
}

const mkcid = c => c instanceof CID ? c : new CID(c)

class ComplexIPLDGraph {
  constructor (store, cbor) {
    this.shardPaths = new Map()
    if (!cbor) {
      cbor = fancyCBOR
    }
    this.cbor = cbor((...args) => store.get(...args))
    this.store = store
    this._clear()
  }
  _clear () {
    this._pending = new Map()
    this._patches = new Map()
    this._bulk = null
  }
  shardPath (path, handler) {
    path = path.split('/').filter(x => x)
    if (path[path.length - 1] !== '*') {
      throw new Error('All shard paths must end in "*".')
    }
    this.shardPaths.set(path, handler)
  }
  async _realKey (path) {
    path = path.split('/').filter(x => x)
    let _realpath = []
    let _shardKeys = new Set(this.shardPaths.keys())

    let i = 0
    while (path.length) {
      let key = path.shift()
      let changed = false
      for (let _path of Array.from(_shardKeys)) {
        let _key = _path[i]
        if (!_key) continue
        if (_key === '*') {
          _realpath.push(await this.shardPaths.get(_path)(key))
          changed = true
          break
        } else if (_key.startsWith(':')) {
          continue
        } else if (_key === key) {
          continue
        } else {
          _shardKeys.delete(_path)
        }
      }
      if (!changed) _realpath.push(key)
      i++
    }
    // handlers can return '/' in keys
    _realpath = _realpath.join('/').split('/')
    return _realpath
  }
  async _kick () {
    if (!this._bulk) this._bulk = await this.store.bulk()
    if (!this._draining) {
      this._draining = (async () => {
        for (let [path, block] of this._pending.entries()) {
          path = await this._realKey(path)
          this._bulk.put(block.cid, block.data)
          nest(this._patches, path, block.cid)
          this._draining = null
        }
      })()
    }
    return this._draining
  }
  _queue (path, block) {
    this._pending.set(path, block)
    this._kick()
  }
  add (path, block) {
    this._queue(path, block)
  }
  async flush (root, clobber = true) {
    if (!root) {
      root = this.root
    }
    if (!root) throw new Error('No root node.')
    root = mkcid(root)
    await this._kick()
    await this._kick()

    let mkcbor = async obj => {
      let cid
      for await (let block of this.cbor.serialize(obj)) {
        this._bulk.put(block.cid, block.data)
        cid = block.cid
      }
      return cid
    }
    let toLink = cid => {
      return {'/': cid.toBaseEncodedString()}
    }

    let _iter = async (map, node) => {
      for (let [key, value] of map.entries()) {
        if (value instanceof Map) {
          let _node
          let cid
          if (node[key]) {
            cid = mkcid(node[key]['/'])
            _node = await this.get(cid)
          } else {
            _node = {}
          }
          node[key] = toLink(await _iter(value, _node))
          if (clobber && cid &&
              node[key]['/'] !== cid.toBaseEncodedString()) {
            this._bulk.del(cid)
          }
        } else {
          if (!(value instanceof CID)) throw new Error('Value not CID.')
          node[key] = toLink(value)
        }
      }
      return mkcbor(node)
    }

    let start = Date.now()
    let cid = await _iter(this._patches, await this.get(root))
    this._graphBuildTime = Date.now() - start

    if (clobber &&
        root.toBaseEncodedString() !== cid.toBaseEncodedString()) {
      this._bulk.del(root)
    }

    start = Date.now()
    await this._bulk.flush()
    this._flushTime = Date.now() - start

    this._clear()

    return cid
  }

  async get (cid) {
    let buffer = await this.store.get(cid)
    return this.cbor.deserialize(buffer)
  }

  async resolve (path, root) {
    path = await this._realKey(path)
    let cid = mkcid(root || this.root)
    while (path.length) {
      cid = mkcid(cid)
      if (cid.codec === 'dag-cbor') {
        let node = await this.get(cid)
        let key = path.shift()
        if (!node[key]) throw new Error('NotFound')
        cid = mkcid(node[key]['/'])
      } else {
        break
      }
    }
    let value
    if (cid.codec === 'dag-cbor') {
      value = await this.get(cid)
    } else {
      value = await this.store.get(cid)
    }
    return {value, remaining: path.join('/')}
  }
}

module.exports = (...args) => new ComplexIPLDGraph(...args)
