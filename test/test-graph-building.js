const {test} = require('tap')
const path = require('path')
const rimraf = require('rimraf')
const createStore = require('ipld-store')
const complex = require('../')
const cbor = require('../fancy-cbor')(() => {})

/* serializer only works for objects with less than 500 keys */
let serialize = async obj => {
  return (await Promise.all(cbor.serialize(obj)))[0]
}
let empty = (async () => {
  let block = await serialize({})
  return block
})()

const graphTest = async (str, cb) => {
  let dir = path.join(__dirname, 'testdb-' + Math.random())
  let store = createStore(dir)
  let block = await empty
  await store.put(block.cid, block.data)
  let graph = complex(store)
  await test(str, async t => {
    t.tearDown(() => {
      rimraf.sync(dir)
    })
    return cb(t, graph)
  })
}

graphTest('basic graph build', async (t, graph) => {
  let block = await serialize({test: 1234})
  graph.add('/one/two/three', block)
  graph.add('/one/three/four', block)
  let newroot = await graph.flush((await empty).cid)
  let i = 0
  for await (let cid of graph.store.cids()) {
    t.ok(cid)
    i++
  }
  t.same(i, 6)
  let two = await graph.resolve('/one/two', newroot)
  t.ok(two.value.three)
  let leaf = await graph.resolve('/one/three/four', newroot)
  t.same(leaf.value, {test: 1234})
})
