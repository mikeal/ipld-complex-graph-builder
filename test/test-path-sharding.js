const {test} = require('tap')
const complex = require('../')

test('test path sharding', async t => {
  let graph = complex()
  graph.shardPath('/one/:two/*', key => {
    t.same(key, 'three')
    return 'four'
  })
  let p = await graph._realKey('/one/two/three')
  t.same(p, ['one', 'two', 'four'])
})

test('test path sharding with slash', async t => {
  let graph = complex()
  graph.shardPath('/one/:two/*', key => {
    t.same(key, 'three')
    return 'four/five'
  })
  let p = await graph._realKey('/one/two/three')
  t.same(p, ['one', 'two', 'four', 'five'])
})

test('test shard two paths', async t => {
  let graph = complex()
  graph.shardPath('/one/:two/*', key => {
    t.same(key, 'three')
    return 'four/five'
  })
  graph.shardPath('/two/:two/*', key => {
    t.same(key, 'three')
    return 'six/seven'
  })
  let p = await graph._realKey('/one/two/three')
  t.same(p, ['one', 'two', 'four', 'five'])
  p = await graph._realKey('/two/two/three')
  t.same(p, ['two', 'two', 'six', 'seven'])
})

test('test shard twice in same path', async t => {
  let graph = complex()
  graph.shardPath('/one/*', key => {
    t.same(key, 'two')
    return 2
  })
  graph.shardPath('/one/:two/*', key => {
    t.same(key, 'three')
    return 3
  })
  let p = await graph._realKey('/one/two/three')
  t.same(p, ['one', '2', '3'])
})
