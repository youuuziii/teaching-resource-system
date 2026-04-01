<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api/client'

const loading = ref(false)
const user = ref(null)

onMounted(async () => {
  loading.value = true
  try {
    const resp = await api.get('/api/me')
    user.value = resp.data.user
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <el-card>
    <template #header>个人中心</template>
    <el-skeleton :loading="loading" animated>
      <template #default>
        <el-descriptions v-if="user" :column="1" border>
          <el-descriptions-item label="用户 ID">{{ user.id }}</el-descriptions-item>
          <el-descriptions-item label="用户名">{{ user.username }}</el-descriptions-item>
          <el-descriptions-item label="角色">{{ (user.roles || []).join(', ') }}</el-descriptions-item>
        </el-descriptions>
      </template>
    </el-skeleton>
  </el-card>
</template>
