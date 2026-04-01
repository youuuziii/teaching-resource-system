<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'

const route = useRoute()
const router = useRouter()

const token = computed(() => localStorage.getItem('token') || '')
const isAuthed = computed(() => token.value.length > 0)

function go(path) {
  router.push(path)
}

function logout() {
  localStorage.removeItem('token')
  localStorage.removeItem('user')
  if (route.path !== '/login') router.push('/login')
}
</script>

<template>
  <el-container style="min-height: 100vh">
    <el-header style="display: flex; align-items: center; gap: 12px">
      <div style="font-weight: 600; cursor: pointer" @click="go('/')">教学资源系统</div>
      <el-menu mode="horizontal" :default-active="route.path" style="flex: 1" @select="go">
        <el-menu-item index="/">资源</el-menu-item>
        <el-menu-item index="/graph">图谱</el-menu-item>
        <el-menu-item index="/profile">个人中心</el-menu-item>
      </el-menu>
      <el-button v-if="!isAuthed" type="primary" @click="go('/login')">登录</el-button>
      <el-button v-else @click="logout">退出</el-button>
    </el-header>
    <el-main>
      <router-view />
    </el-main>
  </el-container>
</template>
