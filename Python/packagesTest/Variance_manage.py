import tensorflow as tf

# 2. 嵌套上下文管理器中的reuse参数的使用

# with tf.variable_scope("root"):
#     print(tf.get_variable_scope().reuse)
#
#     with tf.variable_scope("foo",reuse=True):
#         print(tf.get_variable_scope().reuse)
#
#         with tf.variable_scope("bar"):
#             print(tf.get_variable_scope().reuse)
#
#     print(tf.get_variable_scope().reuse)

# #3. 通过variable_scope来管理变量。
# v1 = tf.get_variable("v", [1])
# print(v1.name)
#
# with tf.variable_scope("foo", reuse=True):
#     v2 = tf.get_variable("v", [1])
# print(v2.name)
#
# with tf.variable_scope("foo"):
#     with tf.variable_scope("bar"):
#         v3 = tf.get_variable("v", [1])
#         print(v3.name)
# v4 = tf.get_variable("v1", [1])
# print(v4.name)


# 1. 保存计算两个变量和的模型。
v1 = tf.Variable(tf.constant(1.0, shape=[1]), name="v1")
v2 = tf.Variable(tf.constant(2.0, shape=[1]), name="v2")
result = v1 + v2

init_op = tf.global_variables_initializer()
saver = tf.train.Saver()

with tf.Session() as sess:
    sess.run(init_op)
    saver.save(sess, "Saved_model/model.ckpt")

# 2. 加载保存了两个变量和的模型。
with tf.Session() as sess:
    saver.restore(sess, "Saved_model/model.ckpt")
    print(sess.run(result))
# 导出TF 计算图的元图，并保存为json格式
saver.export_meta_graph("Saved_model/model.ckpt.meda.json", as_text=True)

# 3. 直接加载持久化的图。 （如果不希望重复定义图上得运算，可采用这种方法）
saver = tf.train.import_meta_graph("Saved_model/model.ckpt.meta")
with tf.Session() as sess:
    saver.restore(sess, "Saved_model/model.ckpt")
    print(sess.run(tf.get_default_graph().get_tensor_by_name("add:0")))

# 4. 变量重命名
v1 = tf.Variable(tf.constant(1.0, shape=[1]), name="other-v1")
v2 = tf.Variable(tf.constant(2.0, shape=[1]), name="other-v2")
saver = tf.train.Saver({"v1": v1, "v2": v2})  # 将上面v1 的值赋给保存的模型中的v1变量
