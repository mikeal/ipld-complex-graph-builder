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

const mkcid = c => c.toBaseEncodedString ? c : new CID(c)

class ComplexIPLDGraph {
  constructor (store, root, cbor) {
    this.shardPaths = new Map()
    if (!cbor) {
      cbor = fancyCBOR
    }
    this.cbor = cbor((...args) => store.get(...args))
    this.store = store
    this.root = root
    this._clear()
    this._bulk = store.bulk()
    this._shardPaths = new Map()
  }
  _clear () {
    this._getCache = new Map()
    this._pending = new Map()
    this._patches = new Map()
  }
  shardPath (path, handler) {
    path = path.split('/').filter(x => x)
    if (path[path.length - 1] !== '*') {
      throw new Error('All shard paths must end in "*".')
    }
    nest(this._shardPaths, path, handler)
  }
  _realKey (path) {
    path = path.split('/').filter(x => x)
    let _realpath = []
    let maps = [this._shardPaths]
    while (path.length) {
      let key = path.shift()
      let nextMaps = []
      let changed = false
      for (let map of maps) {
        for (let [_key, handler] of map.entries()) {
          if (_key === key || _key.startsWith(':')) {
            nextMaps.push(handler)
          }
          if (_key === '*' && !changed) {
            _realpath.push(handler(key))
            changed = true
          }
        }
        maps = nextMaps
      }
      if (!changed) _realpath.push(key)
    }
    return _realpath.join('/').split('/')
  }
  _prime (path) {
    /* pre-fetch intermediate node's we'll need to build the graph */
    path = Array.from(path)
    let run = (parent) => {
      if (path.length) {
        let key = path.shift()
        if (parent[key] && parent[key]['/']) {
          this.get(mkcid(parent[key]['/'])).then(run)
        }
      }
    }
    this.get(this.root).then(run)
  }
  _queue (path, block) {
    this._pending.set(path, block)

    path = this._realKey(path)
    this._prime(path)
    nest(this._patches, path, block.cid)
    this._draining = null
  }
  async add (path, block) {
    if (this._spent) {
      throw new Error('This graph instance has already been flushed.')
    }
    this._queue(path, block)
    return this._bulk.put(block.cid, block.data)
  }
  async flush () {
    if (this._spent) {
      throw new Error('This graph instance has already been flushed.')
    }
    let root = this.root
    if (!root) throw new Error('No root node.')
    root = mkcid(root)

    let mkcbor = async obj => {
      let cid
      for await (let block of this.cbor.serialize(obj)) {
        await this._bulk.put(block.cid, block.data)
        cid = block.cid
      }
      return cid
    }
    let toLink = cid => {
      return {'/': cid.toBaseEncodedString()}
    }

    this.nodesWalked = 0

    let _iter = async (map, node) => {
      this.nodesWalked++
      for (let [key, value] of map.entries()) {
        if (value instanceof Map) {
          let _node
          let cid
          if (node[key] && node[key]['/']) {
            cid = mkcid(node[key]['/'])
            _node = await this.get(cid)
          } else {
            _node = {}
          }
          node[key] = toLink(await _iter(value, _node))
        } else {
          if (!(value.toBaseEncodedString)) throw new Error('Value not CID.')
          node[key] = toLink(value)
        }
      }
      return mkcbor(node)
    }

    let start = Date.now()
    let cid = await _iter(this._patches, await this.get(root))
    this._graphBuildTime = Date.now() - start

    await this._bulk.flush()

    this._clear()
    this._spent = true
    return cid
  }

  get (cid) {
    if (cid.toBaseEncodedString) cid = cid.toBaseEncodedString()
    if (!this._getCache.has(cid)) {
      let p = this.store.get(cid).then(b => this.cbor.deserialize(b))
      this._getCache.set(cid, p)
    }
    return this._getCache.get(cid)
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
