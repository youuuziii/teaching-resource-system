<script setup>
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '../api/client'

const router = useRouter()
const loading = ref(false)
const form = reactive({
  username: '',
  password: '',
})

async function submit() {
  loading.value = true
  try {
    const resp = await api.post('/api/auth/login', {
      username: form.username,
      password: form.password,
    })
    localStorage.setItem('token', resp.data.token)
    localStorage.setItem('user', JSON.stringify(resp.data.user))
    ElMessage.success('登录成功')
    router.push('/')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '登录失败')
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <el-row justify="center">
    <el-col :xs="22" :sm="14" :md="10" :lg="8">
      <el-card>
        <template #header>登录</template>
        <el-form label-width="80px" @submit.prevent="submit">
          <el-form-item label="用户名">
            <el-input v-model="form.username" autocomplete="username" />
          </el-form-item>
          <el-form-item label="密码">
            <el-input v-model="form.password" type="password" autocomplete="current-password" show-password />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" :loading="loading" @click="submit">登录</el-button>
          </el-form-item>
        </el-form>
        <el-alert type="info" show-icon :closable="false">
          <template #default>
            初次使用可直接调用后端 /api/auth/register 注册账号（默认 student 角色），或用数据库手动创建 admin。
          </template>
        </el-alert>
      </el-card>
    </el-col>
  </el-row>
</template>
