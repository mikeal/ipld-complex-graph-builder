const cbor = require('../fancy-cbor')
const {test} = require('tap')

let memget = () => {
  let _get = (cid) => new Promise(resolve => {
    resolve(_get.db.get(cid))
  })
  _get.db = new Map()
  return _get
}

test('basic serialize/deserialize', async t => {
  let _get = memget()
  let ipld = cbor(_get)
  let obj = {test: 1234}
  let iter = ipld.serialize(obj)
  let serialized = Array.from(await Promise.all(await iter))[0]
  t.ok(Buffer.isBuffer(serialized.data))
  t.same(obj, await ipld.deserialize(serialized.data))
})

const bigobj = (size = 10000) => {
  let i = 0
  let o = {}
  while (i < 10000) {
    o['-' + i] = {
      'lasdkjflkasjdf lksajd flkjs dfklajs dlkjsadlkf jaskld f':
      'asdlkjasdl;fkjasldkfjalksdjfklasjdflkajsdlkfjaklfdjslaksfdjkl'
    }
    i++
  }
  return o
}

test('large object serialization/deserialization', async t => {
  let _get = memget()
  let ipld = cbor(_get)
  let o = bigobj()
  let iter = ipld.serialize(o)
  let serialized = Array.from(await Promise.all(await iter))
  t.same(serialized.length, 22)
  serialized.forEach(block => {
    _get.db.set(block.cid.toBaseEncodedString(), block.data)
  })
  t.same(await ipld.deserialize(serialized[21].data), o)
})

test('simple: resolve, cids, tree', async t => {
  let _get = memget()
  let ipld = cbor(_get)
  let write = async obj => {
    let c
    for await (let block of ipld.serialize(obj)) {
      c = block.cid.toBaseEncodedString()
      _get.db.set(c, block.data)
    }
    return c
  }
  let cid = await write({one: {two: {three: 5}}})
  let buff = await _get.db.get(cid)
  t.same((await ipld.resolve(buff, '/one/two/three')).value, 5)
  let iter = await ipld.cids(buff)
  t.same((await Promise.all(iter)).length, 1)
  t.same(Array.from(await ipld.tree(buff)).length, 1)
})

// let obj =
test('linked obj: resolve, cids, tree', async t => {
  let _get = memget()
  let ipld = cbor(_get)
  let write = async obj => {
    let c
    for await (let block of ipld.serialize(obj)) {
      c = block.cid.toBaseEncodedString()
      _get.db.set(c, block.data)
    }
    return c
  }
  let cid = await write({one: {two: {three: 5}}})
  let root = await write({one: {two: {three: {'/': cid}}}})
  let buff = await _get.db.get(root)
  t.same((await ipld.resolve(buff, '/one/two/three/one/two/three')).value, 5)
  let iter = await ipld.cids(buff)
  t.same((await Promise.all(iter)).length, 1)
  t.same(Array.from(await ipld.tree(buff)).length, 1)
})

test('big obj: cids, tree', async t => {
  let _get = memget()
  let ipld = cbor(_get)
  let write = async obj => {
    let c
    for await (let block of ipld.serialize(obj)) {
      c = block.cid.toBaseEncodedString()
      _get.db.set(c, block.data)
    }
    return c
  }
  let root = await write(bigobj())
  let buff = await _get.db.get(root)
  let iter = await ipld.cids(buff)
  t.same((await Promise.all(iter)).length, 22)
  t.same(Array.from(await ipld.tree(buff)).length, 10000)
})
