<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'

const route = useRoute()
const router = useRouter()

const token = computed(() => localStorage.getItem('token') || '')
const isAuthed = computed(() => token.value.length > 0)
const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isSystemAdmin = computed(() => roles.value.includes('admin'))
const isDean = computed(() => roles.value.includes('dean'))
const isTeacherOnly = computed(() => roles.value.includes('teacher') && !roles.value.includes('dean') && !roles.value.includes('admin'))

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
        <el-menu-item v-if="isTeacherOnly" index="/teacher/courses">课程管理</el-menu-item>
        <el-menu-item v-if="isDean" index="/admin/audit">审核</el-menu-item>
        <el-menu-item v-if="isDean || isSystemAdmin" index="/admin/courses">课程与分配</el-menu-item>
        <el-menu-item v-if="isSystemAdmin" index="/admin/logs">日志</el-menu-item>
        <el-menu-item v-if="isSystemAdmin" index="/admin/users">账号权限</el-menu-item>
        <el-menu-item v-else-if="isDean" index="/admin/users">账号管理</el-menu-item>
      </el-menu>
      <el-button v-if="!isAuthed" type="primary" @click="go('/login')">登录</el-button>
      <el-button v-else @click="logout">退出</el-button>
    </el-header>
    <el-main>
      <router-view />
    </el-main>
  </el-container>
</template>
